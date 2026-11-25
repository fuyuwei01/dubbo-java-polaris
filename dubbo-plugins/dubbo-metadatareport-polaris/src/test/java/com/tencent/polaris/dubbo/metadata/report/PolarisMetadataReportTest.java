/*
 * Tencent is pleased to support the open source community by making dubbo-polaris-java available.
 *
 * Copyright (C) 2021 Tencent. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.polaris.dubbo.metadata.report;

import com.tencent.polaris.api.core.ConsumerAPI;
import com.tencent.polaris.api.core.ProviderAPI;
import com.tencent.polaris.api.exception.ErrorCode;
import com.tencent.polaris.api.exception.PolarisException;
import com.tencent.polaris.api.exception.ServerCodes;
import com.tencent.polaris.api.plugin.server.ReportServiceContractRequest;
import com.tencent.polaris.api.pojo.ServiceRule;
import com.tencent.polaris.api.rpc.GetServiceContractRequest;
import com.tencent.polaris.api.rpc.ServiceRuleResponse;
import com.tencent.polaris.common.context.Context;
import com.tencent.polaris.common.config.PolarisConfig;
import com.tencent.polaris.common.registry.PolarisOperator;
import com.tencent.polaris.common.registry.PolarisOperators;
import com.tencent.polaris.specification.api.v1.service.manage.ServiceContractProto;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.JsonUtils;
import org.apache.dubbo.metadata.MappingChangedEvent;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;
import org.apache.dubbo.metadata.report.support.WrapAbstractMetadataReport;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class PolarisMetadataReportTest {

    private static final String SERVICE_KEY = "com.example.DemoService";

    private final URL url = URL.valueOf("polaris://127.0.0.1:8091?file-cache=false&cycle-report=false");

    private ConsumerAPI consumerAPI;
    private ProviderAPI providerAPI;
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> scheduledFuture;
    private PolarisMetadataReport report;

    @Before
    public void setUp() throws Exception {
        consumerAPI = Mockito.mock(ConsumerAPI.class);
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class)))
                .thenReturn(Mockito.mock(ServiceRuleResponse.class));
        providerAPI = Mockito.mock(ProviderAPI.class);
        executor = Mockito.mock(ScheduledExecutorService.class);
        scheduledFuture = Mockito.mock(ScheduledFuture.class);
        Mockito.doReturn(scheduledFuture).when(executor)
                .scheduleAtFixedRate(Mockito.any(Runnable.class), Mockito.eq(5L), Mockito.eq(5L), Mockito.eq(TimeUnit.SECONDS));

        PolarisOperator operator = Mockito.mock(PolarisOperator.class);
        PolarisConfig config = Mockito.mock(PolarisConfig.class);
        Mockito.when(operator.getPolarisConfig()).thenReturn(config);
        Mockito.when(operator.getConsumerAPI()).thenReturn(consumerAPI);
        Mockito.when(operator.getProviderAPI()).thenReturn(providerAPI);
        Mockito.when(config.getNamespace()).thenReturn("default");

        try (MockedStatic<PolarisOperators> operators = Mockito.mockStatic(PolarisOperators.class);
             MockedStatic<MultiReportUtil> multiReport = Mockito.mockStatic(MultiReportUtil.class)) {
            operators.when(() -> PolarisOperators.loadOrStoreForMetaReport(
                    Mockito.anyString(), Mockito.anyInt(), Mockito.anyMap())).thenReturn(operator);
            multiReport.when(() -> MultiReportUtil.buildAnother(Mockito.any(), Mockito.any(URL.class)))
                    .thenReturn(Optional.empty());
            report = new PolarisMetadataReport(url);
        }

        // Drive polling explicitly so the tests do not depend on timer delays.
        Field field = PolarisMetadataReport.class.getDeclaredField("fetchMappingExecutor");
        field.setAccessible(true);
        ((ScheduledExecutorService) field.get(report)).shutdownNow();
        field.set(report, executor);
    }

    @After
    public void tearDown() {
        if (report != null) {
            report.destroy();
        }
    }

    @Test
    public void publishAppMetadataPreservesLegacyContractVersionAndInterfaceVersion() {
        MetadataInfo metadata = appMetadata("revision-1");
        SubscriberMetadataIdentifier identifier = new SubscriberMetadataIdentifier("demo-provider", "revision-1");

        report.publishAppMetadata(identifier, metadata);

        ArgumentCaptor<ReportServiceContractRequest> request = ArgumentCaptor.forClass(ReportServiceContractRequest.class);
        Mockito.verify(providerAPI).reportServiceContract(request.capture());
        ReportServiceContractRequest captured = request.getValue();
        Assert.assertEquals("1.0.0", captured.getVersion());
        Assert.assertEquals("default", captured.getNamespace());
        Assert.assertEquals("demo-provider", captured.getService());
        Assert.assertEquals("dubbo", captured.getProtocol());
        Assert.assertEquals("dubbo::metadata::demo-provider::revision-1", captured.getName());
        Assert.assertEquals(metadata.getContent(), captured.getContent());
        Assert.assertEquals(1, captured.getInterfaceDescriptors().size());
        MetadataInfo.ServiceInfo service = JsonUtils.toJavaObject(
                captured.getInterfaceDescriptors().get(0).getContent(), MetadataInfo.ServiceInfo.class);
        Assert.assertEquals(SERVICE_KEY, service.getName());
        Assert.assertEquals("interface-v2", service.getVersion());
    }

    @Test
    public void appMetadataRevisionsRemainDistinctWithLegacyVersion() {
        try (MockedStatic<Context> context = Mockito.mockStatic(Context.class)) {
            context.when(() -> Context.getFromGlobal(Mockito.anyString(), Mockito.anyString()))
                    .thenReturn("deployment-v3");
            report.publishAppMetadata(new SubscriberMetadataIdentifier("demo-provider", "revision-1"),
                    appMetadata("revision-1"));
            report.publishAppMetadata(new SubscriberMetadataIdentifier("demo-provider", "revision-2"),
                    appMetadata("revision-2"));

            ArgumentCaptor<ReportServiceContractRequest> requests = ArgumentCaptor.forClass(ReportServiceContractRequest.class);
            Mockito.verify(providerAPI, Mockito.times(2)).reportServiceContract(requests.capture());
            Assert.assertEquals("dubbo::metadata::demo-provider::revision-1", requests.getAllValues().get(0).getName());
            Assert.assertEquals("dubbo::metadata::demo-provider::revision-2", requests.getAllValues().get(1).getName());
            requests.getAllValues().forEach(request -> Assert.assertEquals("deployment-v3", request.getVersion()));
        }
    }

    @Test
    public void getAppMetadataPreservesLegacyQueryVersion() {
        MetadataInfo metadata = appMetadata("revision-1");
        MetadataInfo.ServiceInfo service = metadata.getServices().values().iterator().next();
        ServiceContractProto.ServiceContract contract = ServiceContractProto.ServiceContract.newBuilder()
                .addInterfaces(ServiceContractProto.InterfaceDescriptor.newBuilder()
                        .setPath(SERVICE_KEY).setContent(JsonUtils.toJson(service)))
                .build();
        ServiceRuleResponse response = responseWithContract(contract);
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class)))
                .thenReturn(response);

        try (MockedStatic<Context> context = Mockito.mockStatic(Context.class)) {
            context.when(() -> Context.getFromGlobal(Mockito.anyString(), Mockito.anyString()))
                    .thenReturn("consumer-v4");
            SubscriberMetadataIdentifier identifier = new SubscriberMetadataIdentifier("demo-provider", "revision-1");
            for (String providerVersion : new String[]{"", "provider-v2"}) {
                MetadataInfo fetched = report.getAppMetadata(identifier, Collections.singletonMap("version", providerVersion));
                Assert.assertNotNull(fetched);
                Assert.assertEquals("demo-provider", fetched.getApp());
                Assert.assertEquals("revision-1", fetched.getRevision());
                Assert.assertEquals("interface-v2", fetched.getServiceInfo(service.getMatchKey()).getVersion());
            }

            ArgumentCaptor<GetServiceContractRequest> requests = ArgumentCaptor.forClass(GetServiceContractRequest.class);
            Mockito.verify(consumerAPI, Mockito.times(2)).getServiceContract(requests.capture());
            requests.getAllValues().forEach(request -> {
                Assert.assertEquals("consumer-v4", request.getVersion());
                Assert.assertEquals("default", request.getNamespace());
                Assert.assertEquals("demo-provider", request.getService());
                Assert.assertEquals("dubbo", request.getProtocol());
                Assert.assertEquals("dubbo::metadata::demo-provider::revision-1", request.getName());
            });
        }
    }

    @Test
    public void emptyMappingKeepsPollingAndNotifiesWhenProviderAppears() {
        ServiceRuleResponse empty = Mockito.mock(ServiceRuleResponse.class);
        ServiceRuleResponse available = responseWithMapping("", "demo-provider");
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class)))
                .thenReturn(empty, empty, available, available);
        MappingListener listener = Mockito.mock(MappingListener.class);

        Assert.assertTrue(report.getServiceAppMapping(SERVICE_KEY, listener, url).isEmpty());
        Assert.assertTrue(report.getServiceAppMapping(SERVICE_KEY, url).isEmpty());
        Runnable poll = pollingTask();
        poll.run();
        Mockito.verify(listener, Mockito.never()).onEvent(Mockito.any(MappingChangedEvent.class));

        poll.run();
        ArgumentCaptor<MappingChangedEvent> event = ArgumentCaptor.forClass(MappingChangedEvent.class);
        Mockito.verify(listener).onEvent(event.capture());
        Assert.assertEquals(SERVICE_KEY, event.getValue().getServiceKey());
        Assert.assertEquals(Collections.singleton("demo-provider"), event.getValue().getApps());
        Assert.assertEquals(Collections.singleton("demo-provider"), report.getServiceAppMapping(SERVICE_KEY, url));

        poll.run();
        Mockito.verify(listener, Mockito.times(1)).onEvent(Mockito.any(MappingChangedEvent.class));
        Mockito.verify(consumerAPI, Mockito.times(4)).getServiceContract(Mockito.any(GetServiceContractRequest.class));
        pollingTask();
    }

    @Test
    public void missingContractKeepsPolling() {
        ServiceRuleResponse empty = Mockito.mock(ServiceRuleResponse.class);
        Mockito.when(empty.getServiceRule()).thenReturn(Mockito.mock(ServiceRule.class));
        ServiceRuleResponse available = responseWithMapping("revision-1", "demo-provider");
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class)))
                .thenReturn(empty, available);

        Assert.assertTrue(report.getServiceAppMapping(SERVICE_KEY, Mockito.mock(MappingListener.class), url).isEmpty());
        pollingTask().run();
        Assert.assertEquals(Collections.singleton("demo-provider"), report.getServiceAppMapping(SERVICE_KEY, url));
    }

    @Test
    public void existingMappingIsReturnedImmediatelyAndRevisionChangesNotify() {
        ServiceRuleResponse initial = responseWithMapping("revision-1", "first-provider");
        ServiceRuleResponse updated = responseWithMapping("revision-2", "second-provider");
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class)))
                .thenReturn(initial, initial, updated);
        MappingListener listener = Mockito.mock(MappingListener.class);

        Assert.assertEquals(Collections.singleton("first-provider"), report.getServiceAppMapping(SERVICE_KEY, listener, url));
        Runnable poll = pollingTask();
        poll.run();
        Mockito.verify(listener, Mockito.never()).onEvent(Mockito.any(MappingChangedEvent.class));
        poll.run();

        ArgumentCaptor<MappingChangedEvent> event = ArgumentCaptor.forClass(MappingChangedEvent.class);
        Mockito.verify(listener).onEvent(event.capture());
        Assert.assertEquals(Collections.singleton("second-provider"), event.getValue().getApps());
    }

    @Test
    public void concurrentSubscriptionsShareOnePollingTaskAndInitialResult() throws Exception {
        CountDownLatch fetchStarted = new CountDownLatch(1);
        CountDownLatch allowFetch = new CountDownLatch(1);
        CountDownLatch secondQueryStarted = new CountDownLatch(1);
        ServiceRuleResponse available = responseWithMapping("revision-1", "demo-provider");
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class))).thenAnswer(invocation -> {
            fetchStarted.countDown();
            Assert.assertTrue(allowFetch.await(5, TimeUnit.SECONDS));
            return available;
        });
        ExecutorService queries = Executors.newFixedThreadPool(2);
        try {
            Future<Set<String>> first = queries.submit(() -> report.getServiceAppMapping(SERVICE_KEY, Mockito.mock(MappingListener.class), url));
            Assert.assertTrue(fetchStarted.await(5, TimeUnit.SECONDS));
            Future<Set<String>> second = queries.submit(() -> {
                secondQueryStarted.countDown();
                return report.getServiceAppMapping(SERVICE_KEY, Mockito.mock(MappingListener.class), url);
            });
            Assert.assertTrue(secondQueryStarted.await(5, TimeUnit.SECONDS));
            allowFetch.countDown();

            Assert.assertEquals(Collections.singleton("demo-provider"), first.get(5, TimeUnit.SECONDS));
            Assert.assertEquals(Collections.singleton("demo-provider"), second.get(5, TimeUnit.SECONDS));
            Mockito.verify(consumerAPI).getServiceContract(Mockito.any(GetServiceContractRequest.class));
            pollingTask();
        } finally {
            allowFetch.countDown();
            queries.shutdownNow();
            Assert.assertTrue(queries.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void newConsumerReadsLegacyContractAndOldConsumerReadsNewPublication() {
        Map<String, ServiceContractProto.ServiceContract> store = new HashMap<>();
        Mockito.doAnswer(invocation -> {
            ReportServiceContractRequest request = invocation.getArgument(0);
            ServiceContractProto.ServiceContract.Builder contract = ServiceContractProto.ServiceContract.newBuilder();
            request.getInterfaceDescriptors().forEach(descriptor -> contract.addInterfaces(
                    ServiceContractProto.InterfaceDescriptor.newBuilder().setPath(descriptor.getPath()).setContent(descriptor.getContent())));
            store.put(request.getName() + "::" + request.getVersion(), contract.build());
            return null;
        }).when(providerAPI).reportServiceContract(Mockito.any(ReportServiceContractRequest.class));
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class))).thenAnswer(invocation -> {
            GetServiceContractRequest request = invocation.getArgument(0);
            ServiceContractProto.ServiceContract contract = store.get(request.getName() + "::" + request.getVersion());
            return contract == null ? Mockito.mock(ServiceRuleResponse.class) : responseWithContract(contract);
        });
        SubscriberMetadataIdentifier identifier = new SubscriberMetadataIdentifier("demo-provider", "revision-1");
        store.put("dubbo::metadata::demo-provider::revision-1::1.0.0", metadataContract());
        Assert.assertNotNull(report.getAppMetadata(identifier, Collections.emptyMap()));
        store.clear();
        report.publishAppMetadata(identifier, appMetadata("revision-1"));
        GetServiceContractRequest oldQuery = new GetServiceContractRequest();
        oldQuery.setName("dubbo::metadata::demo-provider::revision-1");
        oldQuery.setVersion("1.0.0");
        Assert.assertNotNull(consumerAPI.getServiceContract(oldQuery).getServiceRule());
    }

    @Test
    public void missingLegacyContractFallsBackToEmptyVersion() {
        ServiceRuleResponse response = responseWithContract(metadataContract());
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class)))
                .thenReturn(Mockito.mock(ServiceRuleResponse.class), response);
        Assert.assertNotNull(report.getAppMetadata(new SubscriberMetadataIdentifier("demo-provider", "revision-1"), Collections.emptyMap()));
        ArgumentCaptor<GetServiceContractRequest> requests = ArgumentCaptor.forClass(GetServiceContractRequest.class);
        Mockito.verify(consumerAPI, Mockito.times(2)).getServiceContract(requests.capture());
        Assert.assertEquals("1.0.0", requests.getAllValues().get(0).getVersion());
        Assert.assertEquals("", requests.getValue().getVersion());
    }

    @Test
    public void explicitNotFoundFallsBackButApiTimeoutDoesNot() {
        PolarisException notFound = new PolarisException(ErrorCode.SERVER_USER_ERROR);
        notFound.setServerErrCode(ServerCodes.NOT_FOUND_RESOURCE);
        List<String> versions = new java.util.ArrayList<>();
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class))).thenAnswer(invocation -> {
            GetServiceContractRequest request = invocation.getArgument(0);
            versions.add(request.getVersion());
            if (!request.getVersion().isEmpty()) {
                throw notFound;
            }
            return responseWithContract(metadataContract());
        });
        SubscriberMetadataIdentifier identifier = new SubscriberMetadataIdentifier("demo-provider", "revision-1");
        Assert.assertNotNull(report.getAppMetadata(identifier, Collections.emptyMap()));
        Assert.assertEquals(java.util.Arrays.asList("1.0.0", ""), versions);
        Mockito.reset(consumerAPI);
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class)))
                .thenThrow(new PolarisException(ErrorCode.API_TIMEOUT));
        Assert.assertNull(report.getAppMetadata(identifier, Collections.emptyMap()));
        Mockito.verify(consumerAPI).getServiceContract(Mockito.any(GetServiceContractRequest.class));
    }

    @Test
    public void oneShotQueriesDoNotStartPollingOrRetainEmptyMappings() {
        ServiceRuleResponse response = responseWithMapping("revision-1", "demo-provider");
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class)))
                .thenReturn(Mockito.mock(ServiceRuleResponse.class), response);
        Assert.assertTrue(report.getServiceAppMapping(SERVICE_KEY, url).isEmpty());
        Assert.assertEquals(Collections.singleton("demo-provider"), report.getServiceAppMapping(SERVICE_KEY, url));
        Mockito.verifyNoInteractions(executor);
    }

    @Test
    public void removingLastListenerCancelsPollingAndPreventsFurtherQueries() {
        MappingListener listener = Mockito.mock(MappingListener.class);
        report.getServiceAppMapping(SERVICE_KEY, listener, url);
        Runnable poll = pollingTask();
        report.removeServiceAppMappingListener(SERVICE_KEY, listener);
        report.removeServiceAppMappingListener(SERVICE_KEY, listener);
        Mockito.verify(scheduledFuture).cancel(false);
        poll.run();
        Mockito.verify(consumerAPI).getServiceContract(Mockito.any(GetServiceContractRequest.class));
        Mockito.verifyNoInteractions(listener);
    }

    @Test
    public void duplicateSubscriptionIsIdempotentAndOtherListenersKeepPolling() {
        ServiceRuleResponse response = responseWithMapping("revision-1", "demo-provider");
        Mockito.when(consumerAPI.getServiceContract(Mockito.any(GetServiceContractRequest.class)))
                .thenReturn(Mockito.mock(ServiceRuleResponse.class), response);
        MappingListener first = Mockito.mock(MappingListener.class);
        MappingListener second = Mockito.mock(MappingListener.class);
        report.getServiceAppMapping(SERVICE_KEY, first, url);
        report.getServiceAppMapping(SERVICE_KEY, first, url);
        report.getServiceAppMapping(SERVICE_KEY, second, url);
        Runnable poll = pollingTask();
        report.removeServiceAppMappingListener(SERVICE_KEY, first);
        Mockito.verify(scheduledFuture, Mockito.never()).cancel(Mockito.anyBoolean());
        poll.run();
        Mockito.verify(first, Mockito.never()).onEvent(Mockito.any());
        Mockito.verify(second).onEvent(Mockito.any());
        report.removeServiceAppMappingListener(SERVICE_KEY, second);
        Mockito.verify(scheduledFuture).cancel(false);
    }

    @Test
    public void resubscriptionStartsNewTaskAndOldTaskCannotUpdateIt() {
        MappingListener listener = Mockito.mock(MappingListener.class);
        report.getServiceAppMapping(SERVICE_KEY, listener, url);
        Runnable oldPoll = pollingTask();
        report.removeServiceAppMappingListener(SERVICE_KEY, listener);
        report.getServiceAppMapping(SERVICE_KEY, listener, url);
        oldPoll.run();
        Mockito.verify(consumerAPI, Mockito.times(2)).getServiceContract(Mockito.any());
        Mockito.verify(executor, Mockito.times(2)).scheduleAtFixedRate(Mockito.any(), Mockito.eq(5L), Mockito.eq(5L), Mockito.eq(TimeUnit.SECONDS));
    }

    @Test
    public void cancelledInFlightPollCannotOverwriteNewSubscription() throws Exception {
        MappingListener oldListener = Mockito.mock(MappingListener.class);
        report.getServiceAppMapping(SERVICE_KEY, oldListener, url);
        Runnable oldPoll = pollingTask();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ServiceRuleResponse newMapping = responseWithMapping("new-revision", "new-provider");
        Mockito.when(consumerAPI.getServiceContract(Mockito.any())).thenAnswer(invocation -> {
            started.countDown();
            Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
            return responseWithMapping("old-revision", "old-provider");
        });
        ExecutorService queries = Executors.newSingleThreadExecutor();
        try {
            Future<?> poll = queries.submit(oldPoll);
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
            report.removeServiceAppMappingListener(SERVICE_KEY, oldListener);
            Mockito.doReturn(newMapping).when(consumerAPI).getServiceContract(Mockito.any());
            MappingListener newListener = Mockito.mock(MappingListener.class);
            Assert.assertEquals(Collections.singleton("new-provider"), report.getServiceAppMapping(SERVICE_KEY, newListener, url));
            release.countDown();
            poll.get(5, TimeUnit.SECONDS);
            Assert.assertEquals(Collections.singleton("new-provider"), report.getServiceAppMapping(SERVICE_KEY, url));
            Mockito.verifyNoInteractions(oldListener, newListener);
        } finally {
            release.countDown();
            queries.shutdownNow();
            Assert.assertTrue(queries.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void fallbackListenerIsRemovedUsingTheSameWrapper() throws Exception {
        AbstractMetadataReport fallback = Mockito.mock(AbstractMetadataReport.class);
        Field field = PolarisMetadataReport.class.getDeclaredField("another");
        field.setAccessible(true);
        field.set(report, Optional.of(new WrapAbstractMetadataReport(fallback)));
        Mockito.when(fallback.getServiceAppMapping(Mockito.anyString(), Mockito.any(MappingListener.class), Mockito.any(URL.class)))
                .thenReturn(Collections.emptySet());
        MappingListener listener = Mockito.mock(MappingListener.class);
        report.getServiceAppMapping(SERVICE_KEY, listener, url);
        report.getServiceAppMapping(SERVICE_KEY, listener, url);
        ArgumentCaptor<MappingListener> wrapper = ArgumentCaptor.forClass(MappingListener.class);
        Mockito.verify(fallback).getServiceAppMapping(Mockito.eq(SERVICE_KEY), wrapper.capture(), Mockito.eq(url));
        Assert.assertNotSame(listener, wrapper.getValue());
        report.removeServiceAppMappingListener(SERVICE_KEY, listener);
        Mockito.verify(fallback).removeServiceAppMappingListener(SERVICE_KEY, wrapper.getValue());
        wrapper.getValue().onEvent(new MappingChangedEvent(SERVICE_KEY, Collections.singleton("late-provider")));
        Mockito.verifyNoInteractions(listener);
    }

    @Test
    public void staleFallbackFailureDoesNotRemoveReplacementListener() throws Exception {
        AbstractMetadataReport fallback = Mockito.mock(AbstractMetadataReport.class);
        Field field = PolarisMetadataReport.class.getDeclaredField("another");
        field.setAccessible(true);
        field.set(report, Optional.of(new WrapAbstractMetadataReport(fallback)));
        MappingListener listener = Mockito.mock(MappingListener.class);
        Mockito.when(fallback.getServiceAppMapping(Mockito.eq(SERVICE_KEY), Mockito.any(MappingListener.class), Mockito.eq(url)))
                .thenAnswer(invocation -> {
                    report.removeServiceAppMappingListener(SERVICE_KEY, listener);
                    ServiceRuleResponse replacement = responseWithMapping("new-revision", "new-provider");
                    Mockito.doReturn(replacement).when(consumerAPI).getServiceContract(Mockito.any());
                    report.getServiceAppMapping(SERVICE_KEY, listener, url);
                    throw new IllegalStateException("old fallback failed");
                });
        try {
            report.getServiceAppMapping(SERVICE_KEY, listener, url);
            Assert.fail("Expected the old fallback failure");
        } catch (IllegalStateException e) {
            Assert.assertEquals("old fallback failed", e.getMessage());
        }
        Assert.assertEquals(Collections.singleton("new-provider"), report.getServiceAppMapping(SERVICE_KEY, url));
        Mockito.verify(executor, Mockito.times(2)).scheduleAtFixedRate(Mockito.any(), Mockito.eq(5L), Mockito.eq(5L), Mockito.eq(TimeUnit.SECONDS));
        Mockito.verify(scheduledFuture).cancel(false);
    }

    @Test
    public void schedulingFailureReleasesSubscriptionForRetry() {
        Mockito.doThrow(new java.util.concurrent.RejectedExecutionException("scheduler rejected"))
                .doReturn(scheduledFuture).when(executor)
                .scheduleAtFixedRate(Mockito.any(Runnable.class), Mockito.eq(5L), Mockito.eq(5L), Mockito.eq(TimeUnit.SECONDS));
        MappingListener listener = Mockito.mock(MappingListener.class);
        try {
            report.getServiceAppMapping(SERVICE_KEY, listener, url);
            Assert.fail("Expected the scheduling failure");
        } catch (java.util.concurrent.RejectedExecutionException e) {
            Assert.assertEquals("scheduler rejected", e.getMessage());
        }
        Assert.assertTrue(report.getServiceAppMapping(SERVICE_KEY, listener, url).isEmpty());
        Mockito.verify(consumerAPI, Mockito.times(2)).getServiceContract(Mockito.any());
        report.removeServiceAppMappingListener(SERVICE_KEY, listener);
        Mockito.verify(scheduledFuture).cancel(false);
    }

    @Test
    public void wrapperStopCancelsSubscription() throws Exception {
        AbstractMetadataReport fallback = Mockito.mock(AbstractMetadataReport.class);
        Field field = PolarisMetadataReport.class.getDeclaredField("another");
        field.setAccessible(true);
        field.set(report, Optional.of(new WrapAbstractMetadataReport(fallback)));
        MappingListener listener = Mockito.mock(MappingListener.class);
        report.getServiceAppMapping(SERVICE_KEY, listener, url);
        ArgumentCaptor<MappingListener> wrapper = ArgumentCaptor.forClass(MappingListener.class);
        Mockito.verify(fallback).getServiceAppMapping(Mockito.eq(SERVICE_KEY), wrapper.capture(), Mockito.eq(url));
        wrapper.getValue().stop();
        Mockito.verify(scheduledFuture).cancel(false);
        Mockito.verify(listener).stop();
    }

    @Test
    public void destroyCancelsAllSubscriptionsAndRejectsNewOnes() {
        MappingListener listener = Mockito.mock(MappingListener.class);
        report.getServiceAppMapping(SERVICE_KEY, listener, url);
        Runnable poll = pollingTask();
        report.destroy();
        Mockito.verify(scheduledFuture).cancel(false);
        Mockito.verify(executor).shutdownNow();
        poll.run();
        Assert.assertTrue(report.getServiceAppMapping("other.Service", listener, url).isEmpty());
        Mockito.verify(consumerAPI).getServiceContract(Mockito.any());
    }

    @Test
    public void cancellationDuringInitialFetchDoesNotStartOrRetainTask() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.when(consumerAPI.getServiceContract(Mockito.any())).thenAnswer(invocation -> {
            started.countDown();
            Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
            return responseWithMapping("revision-1", "demo-provider");
        });
        MappingListener listener = Mockito.mock(MappingListener.class);
        ExecutorService queries = Executors.newSingleThreadExecutor();
        try {
            Future<Set<String>> query = queries.submit(() -> report.getServiceAppMapping(SERVICE_KEY, listener, url));
            Assert.assertTrue(started.await(5, TimeUnit.SECONDS));
            report.removeServiceAppMappingListener(SERVICE_KEY, listener);
            release.countDown();
            query.get(5, TimeUnit.SECONDS);
            Mockito.verifyNoInteractions(executor);
            Mockito.verifyNoInteractions(listener);
        } finally {
            release.countDown();
            queries.shutdownNow();
            Assert.assertTrue(queries.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private ServiceContractProto.ServiceContract metadataContract() {
        MetadataInfo.ServiceInfo service = appMetadata("revision-1").getServices().values().iterator().next();
        return ServiceContractProto.ServiceContract.newBuilder().addInterfaces(
                ServiceContractProto.InterfaceDescriptor.newBuilder().setPath(SERVICE_KEY).setContent(JsonUtils.toJson(service))).build();
    }

    private Runnable pollingTask() {
        ArgumentCaptor<Runnable> task = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(executor).scheduleAtFixedRate(task.capture(), Mockito.eq(5L), Mockito.eq(5L), Mockito.eq(TimeUnit.SECONDS));
        return task.getValue();
    }

    private ServiceRuleResponse responseWithMapping(String revision, String application) {
        ServiceContractProto.ServiceContract contract = ServiceContractProto.ServiceContract.newBuilder()
                .setRevision(revision)
                .addInterfaces(ServiceContractProto.InterfaceDescriptor.newBuilder().setPath(application))
                .build();
        return responseWithContract(contract);
    }

    private MetadataInfo appMetadata(String revision) {
        MetadataInfo.ServiceInfo service = new MetadataInfo.ServiceInfo(
                SERVICE_KEY, "demo-group", "interface-v2", "dubbo", 20880, SERVICE_KEY, Collections.emptyMap());
        return new MetadataInfo("demo-provider", revision, Collections.singletonMap(service.getMatchKey(), service));
    }

    private ServiceRuleResponse responseWithContract(ServiceContractProto.ServiceContract contract) {
        ServiceRule rule = Mockito.mock(ServiceRule.class);
        Mockito.when(rule.getRule()).thenReturn(contract);
        ServiceRuleResponse response = Mockito.mock(ServiceRuleResponse.class);
        Mockito.when(response.getServiceRule()).thenReturn(rule);
        return response;
    }
}
