package com.safesparkagents.connect.auth;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link PrincipalPinningInterceptor}. No mockito / no Spark on the classpath: the
 * gRPC plumbing is faked with tiny recording stubs, and the Spark Connect request shape is mimicked
 * by {@link FakeRequest} which exposes the same {@code getUserContext().getUserId()} accessors the
 * interceptor reads by reflection.
 *
 * <p>The interceptor fails CLOSED: with the trusted header present, only a non-blank user_id that
 * equals the principal is allowed through; everything else (absent/blank user_id, null context,
 * unextractable shape, zero identity-bearing messages) is rejected.
 */
class PrincipalPinningInterceptorTest {

    private final PrincipalPinningInterceptor interceptor = new PrincipalPinningInterceptor();

    @Test
    void missingHeader_rejectsAndNeverStartsDownstream() {
        RecordingServerCall<Object, Object> call = new RecordingServerCall<>();
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();

        ServerCall.Listener<Object> listener =
                interceptor.interceptCall(call, new Metadata(), handler);

        assertNotNull(listener, "interceptor must return a (no-op) listener");
        assertFalse(handler.started, "downstream handler must NOT be started for an unauthenticated call");
        assertNotNull(call.closedStatus, "call must be closed");
        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
    }

    @Test
    void mismatchedUserId_rejectsAndDropsMessage() {
        RecordingServerCall<Object, Object> call = new RecordingServerCall<>();
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();

        ServerCall.Listener<Object> listener =
                interceptor.interceptCall(call, headersWithPrincipal("alice"), handler);
        assertTrue(handler.started, "header present -> downstream call is started");

        listener.onMessage(new FakeRequest("bob")); // spoofing attempt

        assertRejected(call, handler);
    }

    @Test
    void matchingUserId_passesThrough() {
        RecordingServerCall<Object, Object> call = new RecordingServerCall<>();
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();

        ServerCall.Listener<Object> listener =
                interceptor.interceptCall(call, headersWithPrincipal("alice"), handler);
        FakeRequest req = new FakeRequest("alice");
        listener.onMessage(req);
        listener.onHalfClose();

        assertNull(call.closedStatus, "a matching identity must not be rejected by the interceptor");
        assertEquals(1, handler.listener.messages.size());
        assertEquals(req, handler.listener.messages.get(0));
        assertTrue(handler.listener.halfClosed, "lifecycle callbacks forwarded on the happy path");
    }

    @Test
    void absentUserId_isRejected() {
        // FAIL CLOSED: an authenticated principal that asserts no user_id must NOT reach Spark.
        RecordingServerCall<Object, Object> call = new RecordingServerCall<>();
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();

        ServerCall.Listener<Object> listener =
                interceptor.interceptCall(call, headersWithPrincipal("alice"), handler);
        listener.onMessage(new FakeRequest(null));

        assertRejected(call, handler);
    }

    @Test
    void blankUserId_isRejected() {
        RecordingServerCall<Object, Object> call = new RecordingServerCall<>();
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();

        ServerCall.Listener<Object> listener =
                interceptor.interceptCall(call, headersWithPrincipal("alice"), handler);
        listener.onMessage(new FakeRequest("   "));

        assertRejected(call, handler);
    }

    @Test
    void nullUserContext_isRejected() {
        RecordingServerCall<Object, Object> call = new RecordingServerCall<>();
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();

        ServerCall.Listener<Object> listener =
                interceptor.interceptCall(call, headersWithPrincipal("alice"), handler);
        listener.onMessage(new NullContextRequest());

        assertRejected(call, handler);
    }

    @Test
    void unextractableRequestShape_isRejected() {
        // No getUserContext()/getUserId() accessors -> reflection failure -> fail closed.
        RecordingServerCall<Object, Object> call = new RecordingServerCall<>();
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();

        ServerCall.Listener<Object> listener =
                interceptor.interceptCall(call, headersWithPrincipal("alice"), handler);
        listener.onMessage(new Object());

        assertRejected(call, handler);
    }

    @Test
    void halfCloseWithoutMessage_isRejected() {
        // A client-streaming RPC that sends no identity-bearing message must not slip through.
        RecordingServerCall<Object, Object> call = new RecordingServerCall<>();
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();

        ServerCall.Listener<Object> listener =
                interceptor.interceptCall(call, headersWithPrincipal("alice"), handler);
        listener.onHalfClose();

        assertNotNull(call.closedStatus, "zero-message request must be rejected");
        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
        assertFalse(handler.listener.halfClosed, "half-close must NOT reach the Spark handler");
    }

    @Test
    void blankHeader_isTreatedAsMissing() {
        RecordingServerCall<Object, Object> call = new RecordingServerCall<>();
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();
        Metadata headers = new Metadata();
        headers.put(PrincipalPinningInterceptor.PRINCIPAL_KEY, "   ");

        interceptor.interceptCall(call, headers, handler);

        assertFalse(handler.started);
        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
    }

    @Test
    void identityNeutralMethod_bypassesUserIdPin() {
        // Health/reflection RPCs carry no user_id; authenticated by the header, exempt from the pin.
        RecordingServerCall<Object, Object> call =
                new RecordingServerCall<>("grpc.health.v1.Health/Check");
        CapturingHandler<Object, Object> handler = new CapturingHandler<>();

        ServerCall.Listener<Object> listener =
                interceptor.interceptCall(call, headersWithPrincipal("alice"), handler);
        listener.onMessage(new FakeRequest(null)); // no user_id, but method is identity-neutral
        listener.onHalfClose();

        assertNull(call.closedStatus, "identity-neutral method must not be rejected for lack of user_id");
        assertEquals(1, handler.listener.messages.size());
        assertTrue(handler.listener.halfClosed);
    }

    @Test
    void identityNeutralAllowlist_excludesSparkConnectMethods() {
        assertFalse(PrincipalPinningInterceptor.IDENTITY_NEUTRAL_METHODS.stream()
                .anyMatch(m -> m.startsWith("spark.connect.")));
        assertTrue(PrincipalPinningInterceptor.IDENTITY_NEUTRAL_METHODS
                .contains("grpc.health.v1.Health/Check"));
    }

    @Test
    void extractUserId_readsNestedAccessorsAndTrims() {
        assertEquals("agent_42", PrincipalPinningInterceptor.extractUserId(new FakeRequest(" agent_42 ")));
        assertNull(PrincipalPinningInterceptor.extractUserId(new FakeRequest(null)));
        assertNull(PrincipalPinningInterceptor.extractUserId(new FakeRequest("   ")));
        assertNull(PrincipalPinningInterceptor.extractUserId(new NullContextRequest()));
        assertNull(PrincipalPinningInterceptor.extractUserId(new Object())); // no accessor -> null
        assertNull(PrincipalPinningInterceptor.extractUserId(null));
    }

    // --- helpers --------------------------------------------------------------------------------

    private static Metadata headersWithPrincipal(String principal) {
        Metadata m = new Metadata();
        m.put(PrincipalPinningInterceptor.PRINCIPAL_KEY, principal);
        return m;
    }

    private static void assertRejected(RecordingServerCall<Object, Object> call,
            CapturingHandler<Object, Object> handler) {
        assertNotNull(call.closedStatus, "request must be rejected");
        assertEquals(Status.Code.UNAUTHENTICATED, call.closedStatus.getCode());
        assertTrue(handler.listener.messages.isEmpty(),
                "an unverified message must NOT reach the Spark handler");
    }

    // --- test fixtures --------------------------------------------------------------------------

    /** Mimics a Spark Connect request proto: getUserContext().getUserId(). */
    public static final class FakeRequest {
        private final FakeUserContext ctx;

        FakeRequest(String userId) {
            this.ctx = new FakeUserContext(userId);
        }

        public FakeUserContext getUserContext() {
            return ctx;
        }
    }

    public static final class FakeUserContext {
        private final String userId;

        FakeUserContext(String userId) {
            this.userId = userId;
        }

        public String getUserId() {
            return userId;
        }
    }

    /** A request whose user context is null (e.g. a malformed/empty message). */
    public static final class NullContextRequest {
        public FakeUserContext getUserContext() {
            return null;
        }
    }

    private static final class RecordingServerCall<ReqT, RespT> extends ServerCall<ReqT, RespT> {
        private final String fullMethodName;
        Status closedStatus;

        RecordingServerCall() {
            this(null);
        }

        RecordingServerCall(String fullMethodName) {
            this.fullMethodName = fullMethodName;
        }

        @Override public void request(int numMessages) { }

        @Override public void sendHeaders(Metadata headers) { }

        @Override public void sendMessage(RespT message) { }

        @Override public void close(Status status, Metadata trailers) { this.closedStatus = status; }

        @Override public boolean isCancelled() { return false; }

        @Override
        public MethodDescriptor<ReqT, RespT> getMethodDescriptor() {
            if (fullMethodName == null) {
                return null;
            }
            return MethodDescriptor.<ReqT, RespT>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNKNOWN)
                    .setFullMethodName(fullMethodName)
                    .setRequestMarshaller(new NoopMarshaller<>())
                    .setResponseMarshaller(new NoopMarshaller<>())
                    .build();
        }
    }

    private static final class NoopMarshaller<T> implements MethodDescriptor.Marshaller<T> {
        @Override public InputStream stream(T value) { return new ByteArrayInputStream(new byte[0]); }

        @Override public T parse(InputStream stream) { return null; }
    }

    private static final class CapturingHandler<ReqT, RespT>
            implements ServerCallHandler<ReqT, RespT> {
        boolean started = false;
        final RecordingListener<ReqT> listener = new RecordingListener<>();

        @Override
        public ServerCall.Listener<ReqT> startCall(ServerCall<ReqT, RespT> call, Metadata headers) {
            started = true;
            return listener;
        }
    }

    private static final class RecordingListener<ReqT> extends ServerCall.Listener<ReqT> {
        final List<ReqT> messages = new ArrayList<>();
        boolean halfClosed = false;

        @Override public void onMessage(ReqT message) { messages.add(message); }

        @Override public void onHalfClose() { halfClosed = true; }
    }
}
