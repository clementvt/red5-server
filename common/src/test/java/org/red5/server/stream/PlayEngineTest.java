package org.red5.server.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.red5.server.api.scheduling.IScheduledJob;
import org.red5.server.api.scheduling.ISchedulingService;
import org.red5.server.api.scope.IBroadcastScope;
import org.red5.server.api.scope.IScope;
import org.red5.server.api.stream.IPlayItem;
import org.red5.server.api.stream.ISubscriberStream;
import org.red5.server.api.stream.StreamState;
import org.red5.server.api.stream.support.SimplePlayItem;
import org.red5.server.messaging.IMessage;
import org.red5.server.messaging.IMessageInput;
import org.red5.server.messaging.IMessageOutput;
import org.red5.server.net.rtmp.status.StatusCodes;
import org.red5.server.stream.IProviderService.INPUT_TYPE;
import org.red5.server.stream.message.StatusMessage;

public class PlayEngineTest {

    @Test
    public void testPlayThrowsWhenStateIsNotStopped() throws Exception {
        EngineFixture fixture = newFixture(StreamState.PLAYING);
        IPlayItem item = SimplePlayItem.build("stream", -1000, 0);

        try {
            fixture.engine.play(item, true);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertEquals("Cannot play from non-stopped state", e.getMessage());
        }
    }

    @Test
    public void testPlayNotFoundUnsubscribesCurrentInputAndSendsStatus() throws Exception {
        EngineFixture fixture = newFixture(StreamState.STOPPED);
        fixture.lookupType.set(INPUT_TYPE.NOT_FOUND);
        setPrivateAtomicReference(fixture.engine, "msgInReference", fixture.existingInput);
        IPlayItem item = SimplePlayItem.build("missing", -1000, 0);

        try {
            fixture.engine.play(item, true);
            fail("Expected StreamNotFoundException");
        } catch (StreamNotFoundException e) {
            assertEquals("Stream missing not found", e.getMessage());
        }

        assertEquals("Existing input should be unsubscribed in checkPlayStateStopped", 1,
                fixture.existingUnsubscribeCount.get());
        assertTrue("Expected a status message",
                fixture.pushedMessages.stream().anyMatch(StatusMessage.class::isInstance));
        StatusMessage statusMessage = (StatusMessage) fixture.pushedMessages.stream()
                .filter(StatusMessage.class::isInstance).findFirst().orElse(null);
        assertNotNull(statusMessage);
        assertEquals(StatusCodes.NS_PLAY_STREAMNOTFOUND, statusMessage.getBody().getCode());
    }

    @Test
    public void testPlayWaitDecisionSchedulesLiveWaitJobForTypeMinusTwo() throws Exception {
        EngineFixture fixture = newFixture(StreamState.STOPPED);
        fixture.lookupType.set(INPUT_TYPE.LIVE_WAIT);
        fixture.liveInput.set(createMessageInputProxy(false, new AtomicInteger(), new AtomicInteger()));
        IPlayItem item = SimplePlayItem.build("live-wait", -2000, 0);

        fixture.engine.play(item, true);

        assertEquals(15000L, fixture.lastScheduledDelay.get().longValue());
        assertNotNull(getPrivateField(fixture.engine, "waitLiveJob"));
        assertEquals(1, fixture.onChangeCount.get());
        assertEquals(StreamState.PLAYING, fixture.lastOnChangeState.get());
    }

    @Test
    public void testPlayVodDecisionSubscribesAndPushesPlayingChange() throws Exception {
        EngineFixture fixture = newFixture(StreamState.STOPPED);
        fixture.lookupType.set(INPUT_TYPE.VOD);
        AtomicInteger subscribeCount = new AtomicInteger();
        fixture.vodInput.set(createMessageInputProxy(false, subscribeCount, new AtomicInteger()));
        IPlayItem item = SimplePlayItem.build("vod", 1000, 0);

        fixture.engine.play(item, true);

        assertEquals(1, subscribeCount.get());
        assertEquals(1, fixture.onChangeCount.get());
        assertEquals(StreamState.PLAYING, fixture.lastOnChangeState.get());
    }

    @Test
    public void testPlayLiveDecisionSubscribesAndSetsPlayingState() throws Exception {
        EngineFixture fixture = newFixture(StreamState.STOPPED);
        fixture.lookupType.set(INPUT_TYPE.LIVE);
        AtomicInteger subscribeCount = new AtomicInteger();
        fixture.liveInput.set(createMessageInputProxy(true, subscribeCount, new AtomicInteger()));
        IPlayItem item = SimplePlayItem.build("live", -2000, 0);

        fixture.engine.play(item, true);

        assertEquals(1, subscribeCount.get());
        assertEquals(StreamState.PLAYING, fixture.state.get());
        assertEquals(1, fixture.onChangeCount.get());
        assertEquals(StreamState.PLAYING, fixture.lastOnChangeState.get());
    }

    private static EngineFixture newFixture(StreamState initialState) {
        EngineFixture fixture = new EngineFixture();
        fixture.state.set(initialState);

        IScope scope = proxy(IScope.class, (proxy, method, args) -> defaultReturn(method));
        ISubscriberStream subscriberStream = proxy(ISubscriberStream.class, (proxy, method, args) -> {
            String name = method.getName();
            if ("getStreamId".equals(name)) {
                return 1;
            }
            if ("getScope".equals(name)) {
                return scope;
            }
            if ("getState".equals(name)) {
                return fixture.state.get();
            }
            if ("setState".equals(name)) {
                fixture.state.set((StreamState) args[0]);
                return null;
            }
            if ("onChange".equals(name)) {
                fixture.onChangeCount.incrementAndGet();
                fixture.lastOnChangeState.set((StreamState) args[0]);
                return null;
            }
            if ("scheduleWithFixedDelay".equals(name)) {
                return "pull-job";
            }
            if ("getClientBufferDuration".equals(name)) {
                return 0;
            }
            return defaultReturn(method);
        });

        ISchedulingService schedulingService = proxy(ISchedulingService.class, (proxy, method, args) -> {
            if ("addScheduledOnceJob".equals(method.getName()) && args != null && args.length == 2
                    && args[0] instanceof Long && args[1] instanceof IScheduledJob) {
                fixture.lastScheduledDelay.set((Long) args[0]);
                return "wait-live-job";
            }
            return defaultReturn(method);
        });

        IConsumerService consumerService = proxy(IConsumerService.class,
                (proxy, method, args) -> defaultReturn(method));

        IProviderService providerService = proxy(IProviderService.class, (proxy, method, args) -> {
            String name = method.getName();
            if ("lookupProviderInput".equals(name)) {
                return fixture.lookupType.get();
            }
            if ("getLiveProviderInput".equals(name)) {
                return fixture.liveInput.get();
            }
            if ("getVODProviderInput".equals(name)) {
                return fixture.vodInput.get();
            }
            return defaultReturn(method);
        });

        fixture.existingInput = createMessageInputProxy(false, new AtomicInteger(), fixture.existingUnsubscribeCount);
        fixture.vodInput.set(createMessageInputProxy(false, new AtomicInteger(), new AtomicInteger()));
        fixture.liveInput.set(createMessageInputProxy(true, new AtomicInteger(), new AtomicInteger()));

        fixture.engine = new PlayEngine.Builder(subscriberStream, schedulingService, consumerService, providerService)
                .build();
        fixture.engine.setMessageOut(createMessageOutputProxy(fixture.pushedMessages));
        return fixture;
    }

    private static IMessageInput createMessageInputProxy(boolean broadcastScope, AtomicInteger subscribeCount,
            AtomicInteger unsubscribeCount) {
        InvocationHandler handler = (proxy, method, args) -> {
            String name = method.getName();
            if ("subscribe".equals(name)) {
                subscribeCount.incrementAndGet();
                return true;
            }
            if ("unsubscribe".equals(name)) {
                unsubscribeCount.incrementAndGet();
                return true;
            }
            if ("pullMessage".equals(name)) {
                return null;
            }
            if ("sendOOBControlMessage".equals(name)) {
                return null;
            }
            if ("getClientBroadcastStream".equals(name)) {
                return null;
            }
            return defaultReturn(method);
        };
        if (broadcastScope) {
            return (IMessageInput) Proxy.newProxyInstance(PlayEngineTest.class.getClassLoader(),
                    new Class[] { IMessageInput.class, IBroadcastScope.class }, handler);
        }
        return proxy(IMessageInput.class, handler);
    }

    private static IMessageOutput createMessageOutputProxy(List<IMessage> pushedMessages) {
        return proxy(IMessageOutput.class, (proxy, method, args) -> {
            if ("pushMessage".equals(method.getName()) && args != null && args.length == 1
                    && args[0] instanceof IMessage) {
                pushedMessages.add((IMessage) args[0]);
                return null;
            }
            if ("subscribe".equals(method.getName()) || "unsubscribe".equals(method.getName())) {
                return true;
            }
            return defaultReturn(method);
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(PlayEngineTest.class.getClassLoader(), new Class[] { type }, handler);
    }

    private static Object defaultReturn(Method method) {
        Class<?> returnType = method.getReturnType();
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (boolean.class == returnType) {
            return false;
        }
        if (byte.class == returnType) {
            return (byte) 0;
        }
        if (short.class == returnType) {
            return (short) 0;
        }
        if (int.class == returnType) {
            return 0;
        }
        if (long.class == returnType) {
            return 0L;
        }
        if (float.class == returnType) {
            return 0f;
        }
        if (double.class == returnType) {
            return 0d;
        }
        if (char.class == returnType) {
            return '\0';
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static void setPrivateAtomicReference(PlayEngine engine, String fieldName, Object value) throws Exception {
        Field field = PlayEngine.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        ((AtomicReference<Object>) field.get(engine)).set(value);
    }

    private static Object getPrivateField(PlayEngine engine, String fieldName) throws Exception {
        Field field = PlayEngine.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(engine);
    }

    private static final class EngineFixture {
        private final AtomicReference<StreamState> state = new AtomicReference<>(StreamState.STOPPED);

        private final AtomicInteger onChangeCount = new AtomicInteger();

        private final AtomicReference<StreamState> lastOnChangeState = new AtomicReference<>();

        private final AtomicReference<INPUT_TYPE> lookupType = new AtomicReference<>(INPUT_TYPE.NOT_FOUND);

        private final AtomicReference<IMessageInput> liveInput = new AtomicReference<>();

        private final AtomicReference<IMessageInput> vodInput = new AtomicReference<>();

        private final AtomicReference<Long> lastScheduledDelay = new AtomicReference<>();

        private final AtomicInteger existingUnsubscribeCount = new AtomicInteger();

        private final List<IMessage> pushedMessages = new ArrayList<>();

        private IMessageInput existingInput;

        private PlayEngine engine;
    }
}
