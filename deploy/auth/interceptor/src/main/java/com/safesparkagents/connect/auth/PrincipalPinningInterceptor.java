package com.safesparkagents.connect.auth;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import java.lang.reflect.Method;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Pins the Spark Connect session identity to a cryptographically verified principal.
 *
 * <p>Option A architecture: a fronting Envoy proxy terminates client mTLS, verifies the client
 * certificate against the Connect-layer CA, and injects the verified identity into a trusted
 * metadata header ({@value #PRINCIPAL_HEADER}). Envoy strips any client-supplied copy of that
 * header before setting it, so inside the server the header is non-spoofable: its mere presence
 * proves the request traversed the proxy and presented a valid client cert.
 *
 * <p>This interceptor <b>fails closed</b>. On every RPC it enforces:
 * <ol>
 *   <li><b>No bypass.</b> If {@value #PRINCIPAL_HEADER} is absent the request did not come through
 *       Envoy (a direct dial to 127.0.0.1:15002), so it is rejected with
 *       {@link Status#UNAUTHENTICATED}.</li>
 *   <li><b>Identity is pinned.</b> Every Spark Connect data-plane RPC carries a client-asserted
 *       {@code user_id} in the {@code UserContext} of each request message. The RPC is rejected
 *       unless a <b>non-blank</b> {@code user_id} is present and <b>equals</b> the verified
 *       principal. Absent {@code user_id}, blank {@code user_id}, a null {@code UserContext}, an
 *       unknown request shape, any reflection failure during extraction, or a request that sends no
 *       identity-bearing message at all are ALL rejected — never forwarded unchecked. This kills the
 *       native spoofing vector (a client setting {@code ;user_id=someone_else}) and closes the
 *       fail-open hole where a missing/blank {@code user_id} would otherwise skip the check.</li>
 * </ol>
 *
 * <p><b>Identity-neutral methods.</b> A small, explicit {@link #IDENTITY_NEUTRAL_METHODS} allowlist
 * exempts standard gRPC <i>infrastructure</i> services (health, reflection) that legitimately carry
 * no {@code UserContext}. They are still gated by the presence of the verified principal header (so
 * they cannot be reached without a valid client cert through Envoy); they are merely exempt from the
 * {@code user_id}-equals-principal pin because they have no {@code user_id} to assert. Every Spark
 * Connect data-plane RPC carries {@code user_context.user_id} and is therefore NOT on this list, so
 * it is pinned and fails closed.
 *
 * <p>The {@code user_id} is read by reflection against the request message's
 * {@code getUserContext().getUserId()} accessors. This deliberately avoids a compile-time
 * dependency on the Spark Connect protobuf classes: it works uniformly across every Connect request
 * type (ExecutePlan, AnalyzePlan, Config, AddArtifacts, Interrupt, ReattachExecute, ReleaseExecute,
 * FetchErrorDetails, ...) and keeps the build hermetic and the shaded jar free of a second copy of
 * grpc/protobuf that would collide with the server's own classpath.
 *
 * <p>Wiring (see ../README.md): drop the shaded jar on the Connect server classpath and set
 * {@code spark.connect.grpc.interceptor.classes=com.safesparkagents.connect.auth.PrincipalPinningInterceptor}.
 * Spark instantiates interceptor classes via their <b>zero-argument constructor</b>, which this class
 * provides.
 */
public final class PrincipalPinningInterceptor implements ServerInterceptor {

    /** Trusted, Envoy-injected header carrying the verified client-cert identity. */
    public static final String PRINCIPAL_HEADER = "x-connect-principal";

    static final Metadata.Key<String> PRINCIPAL_KEY =
            Metadata.Key.of(PRINCIPAL_HEADER, Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Standard gRPC infrastructure RPCs that carry no Spark {@code UserContext} and are therefore
     * exempt from the user_id pin (but NOT from the header-presence/authentication check). These are
     * identity-neutral: health probes and schema reflection expose no per-principal data. Spark
     * Connect's own service methods are intentionally absent — they all carry user_id and must pin.
     */
    static final Set<String> IDENTITY_NEUTRAL_METHODS = Set.of(
            "grpc.health.v1.Health/Check",
            "grpc.health.v1.Health/Watch",
            "grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo");

    private static final Logger LOG = Logger.getLogger(PrincipalPinningInterceptor.class.getName());

    /** Required zero-arg constructor for {@code spark.connect.grpc.interceptor.classes}. */
    public PrincipalPinningInterceptor() {
        LOG.info("PrincipalPinningInterceptor active: pinning Spark Connect user_id to the "
                + PRINCIPAL_HEADER + " header set by the mTLS auth proxy (fail-closed)");
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        final String principal = trimToNull(headers.get(PRINCIPAL_KEY));
        if (principal == null) {
            return deny(call, "missing trusted '" + PRINCIPAL_HEADER + "' header; the request did "
                    + "not traverse the mTLS auth proxy (direct connections to the Connect port are "
                    + "not authenticated)");
        }

        // Identity-neutral infrastructure RPCs: authenticated by the header, but carry no user_id.
        final MethodDescriptor<ReqT, RespT> method = call.getMethodDescriptor();
        if (method != null && IDENTITY_NEUTRAL_METHODS.contains(method.getFullMethodName())) {
            return next.startCall(call, headers);
        }

        final ServerCall.Listener<ReqT> delegate = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(delegate) {
            private boolean rejected = false;
            private boolean pinned = false;

            @Override
            public void onMessage(ReqT message) {
                if (rejected) {
                    return;
                }
                String userId = extractUserId(message);
                if (userId == null) {
                    reject("Spark Connect request asserted no usable user_id (absent/blank "
                            + "user_id, null UserContext, or unextractable request shape); cannot "
                            + "pin identity to verified principal '" + principal + "'");
                    return;
                }
                if (!userId.equals(principal)) {
                    reject("Spark Connect user_id '" + userId + "' does not match the verified "
                            + "client-certificate principal '" + principal + "'");
                    return;
                }
                pinned = true;
                super.onMessage(message);
            }

            @Override
            public void onHalfClose() {
                if (rejected) {
                    return;
                }
                // Fail closed: a request that half-closes without ever asserting a matching user_id
                // (e.g. a client-streaming RPC with zero request messages) is never let through.
                if (!pinned) {
                    reject("Spark Connect request completed without asserting a user_id that "
                            + "matches the verified principal '" + principal + "'");
                    return;
                }
                super.onHalfClose();
            }

            @Override
            public void onCancel() {
                if (rejected) {
                    return;
                }
                super.onCancel();
            }

            @Override
            public void onComplete() {
                if (rejected) {
                    return;
                }
                super.onComplete();
            }

            @Override
            public void onReady() {
                if (rejected) {
                    return;
                }
                super.onReady();
            }

            private void reject(String description) {
                rejected = true;
                LOG.warning("rejecting RPC: " + description);
                call.close(Status.UNAUTHENTICATED.withDescription(description), new Metadata());
            }
        };
    }

    private static <ReqT, RespT> ServerCall.Listener<ReqT> deny(ServerCall<ReqT, RespT> call,
            String description) {
        LOG.warning("rejecting RPC: " + description);
        call.close(Status.UNAUTHENTICATED.withDescription(description), new Metadata());
        return new ServerCall.Listener<ReqT>() { };
    }

    /**
     * Extracts {@code user_id} from a Spark Connect request message via
     * {@code getUserContext().getUserId()}, by reflection. Returns {@code null} (which the caller
     * treats as a hard rejection) when the message is null, has no user context accessor, has a null
     * user context, asserts no/blank user id, or any reflection step fails. Callers must NOT forward
     * a request whose extraction returned {@code null}.
     */
    static String extractUserId(Object message) {
        if (message == null) {
            return null;
        }
        try {
            Method getUserContext = message.getClass().getMethod("getUserContext");
            Object userContext = getUserContext.invoke(message);
            if (userContext == null) {
                return null;
            }
            Method getUserId = userContext.getClass().getMethod("getUserId");
            Object userId = getUserId.invoke(userContext);
            return userId == null ? null : trimToNull(userId.toString());
        } catch (ReflectiveOperationException | RuntimeException e) {
            // Unknown request shape / extraction failure: fail closed (return null -> reject).
            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "could not extract user_id from " + message.getClass().getName(), e);
            }
            return null;
        }
    }

    private static String trimToNull(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.isEmpty() ? null : t;
    }
}
