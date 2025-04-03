//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.smallrye.stork.servicediscovery.kubernetes;

import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointPort;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.dsl.AnyNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.smallrye.mutiny.Uni;
import io.smallrye.stork.api.Metadata;
import io.smallrye.stork.api.ServiceInstance;
import io.smallrye.stork.impl.CachingServiceDiscovery;
import io.smallrye.stork.impl.DefaultServiceInstance;
import io.smallrye.stork.utils.ServiceInstanceIds;
import io.smallrye.stork.utils.ServiceInstanceUtils;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesServiceDiscovery extends CachingServiceDiscovery {
    static final String METADATA_NAME = "metadata.name";
    private final KubernetesClient client;
    private final String application;
    private final String portName;
    private final boolean allNamespaces;
    private final String namespace;
    private final boolean secure;
    private final Vertx vertx;
    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesServiceDiscovery.class);
    private final KubernetesClient clientW;
    private AtomicBoolean invalidated = new AtomicBoolean();

    public KubernetesServiceDiscovery(String serviceName, KubernetesConfiguration config, Vertx vertx) {
        super(config.getRefreshPeriod());
        Config base = Config.autoConfigure((String)null);
        String masterUrl = config.getK8sHost() == null ? base.getMasterUrl() : config.getK8sHost();
        this.application = config.getApplication() == null ? serviceName : config.getApplication();
        this.namespace = config.getK8sNamespace() == null ? base.getNamespace() : config.getK8sNamespace();
        this.portName = config.getPortName();
        this.allNamespaces = this.namespace != null && this.namespace.equalsIgnoreCase("all");
        if (this.namespace == null) {
            throw new IllegalArgumentException("Namespace is not configured for service '" + serviceName + "'. Please provide a namespace. Use 'all' to discover services in all namespaces");
        } else {
            Config k8sConfig = ((ConfigBuilder)((ConfigBuilder)(new ConfigBuilder(base)).withMasterUrl(masterUrl)).withNamespace(this.namespace)).build();
            this.client = (new KubernetesClientBuilder()).withConfig(k8sConfig).build();
            this.clientW = (new KubernetesClientBuilder()).withConfig(k8sConfig).build();
            this.vertx = vertx;
            this.secure = isSecure(config);
            AnyNamespaceOperation<Endpoints, EndpointsList, Resource<Endpoints>> endpointsOperation;
            if (this.allNamespaces) {
                endpointsOperation = (AnyNamespaceOperation)this.clientW.endpoints().inAnyNamespace();
            } else {
                endpointsOperation = (AnyNamespaceOperation)this.clientW.endpoints().inNamespace(this.namespace);
            }

            endpointsOperation.withField(METADATA_NAME, application).inform(new ResourceEventHandler<Endpoints>() {
                public void onAdd(Endpoints obj) {
                    KubernetesServiceDiscovery.LOGGER.info("Endpoint added: {}", obj.getMetadata().getName());
                    KubernetesServiceDiscovery.this.invalidate();
                }

                public void onUpdate(Endpoints oldObj, Endpoints newObj) {
                    KubernetesServiceDiscovery.LOGGER.info("Endpoint updated : {}", newObj.getMetadata().getName());
                    KubernetesServiceDiscovery.this.invalidate();
                }

                public void onDelete(Endpoints obj, boolean deletedFinalStateUnknown) {
                    KubernetesServiceDiscovery.LOGGER.info("Endpoint deleted: {}", obj.getMetadata().getName());
                    KubernetesServiceDiscovery.this.invalidate();
                }
            });
        }
    }

    public Uni<List<ServiceInstance>> cache(Uni<List<ServiceInstance>> uni) {
        return uni.memoize().until(() -> this.invalidated.get());
    }

    public void invalidate() {
        this.invalidated.set(true);
    }

    public Uni<List<ServiceInstance>> fetchNewServiceInstances(List<ServiceInstance> previousInstances) {
        KubernetesServiceDiscovery.LOGGER.info("Fetching new service instances");
        invalidated.set(false);
        Uni<Map<Endpoints, List<Pod>>> endpointsUni = Uni.createFrom().emitter((emitter) -> this.vertx.executeBlocking((future) -> {
            Map<Endpoints, List<Pod>> items = new HashMap();
            if (this.allNamespaces) {
                for(Endpoints endpoint : ((EndpointsList)((FilterWatchListDeletable)((AnyNamespaceOperation)this.client.endpoints().inAnyNamespace()).withField("metadata.name", this.application)).list()).getItems()) {
                    List<Pod> backendPods = new ArrayList();
                    List<String> podNames = (List)endpoint.getSubsets().stream().flatMap((endpointSubset) -> endpointSubset.getAddresses().stream()).map((address) -> address.getTargetRef().getName()).collect(Collectors.toList());
                    podNames.forEach((podName) -> backendPods.addAll(((PodList)((FilterWatchListDeletable)((AnyNamespaceOperation)this.client.pods().inAnyNamespace()).withField("metadata.name", podName)).list()).getItems()));
                    items.put(endpoint, backendPods);
                }
            } else {
                for(Endpoints endpoint : ((EndpointsList)((FilterWatchListDeletable)((NonNamespaceOperation)this.client.endpoints().inNamespace(this.namespace)).withField("metadata.name", this.application)).list()).getItems()) {
                    List<Pod> backendPods = new ArrayList();
                    List<String> podNames = (List)endpoint.getSubsets().stream().flatMap((endpointSubset) -> endpointSubset.getAddresses().stream()).map((address) -> address.getTargetRef().getName()).collect(Collectors.toList());
                    backendPods.addAll((Collection)podNames.stream().map((name) -> (PodResource)((NonNamespaceOperation)this.client.pods().inNamespace(this.namespace)).withName(name)).map((podPodResource) -> (Pod)podPodResource.get()).collect(Collectors.toList()));
                    items.put(endpoint, backendPods);
                }
            }

            future.complete(items);
        }, (result) -> {
            if (result.succeeded()) {
                Map<Endpoints, List<Pod>> endpoints = (Map)result.result();
                emitter.complete(endpoints);
            } else {
                LOGGER.error("Unable to retrieve the endpoint from the {} service", this.application, result.cause());
                emitter.fail(result.cause());
                this.invalidated.set(true);
            }

        }));
        return endpointsUni.onItem().transform((endpoints) -> this.toStorkServiceInstances(endpoints, previousInstances)).invoke(() -> this.invalidated.set(false));
    }

    private List<ServiceInstance> toStorkServiceInstances(Map<Endpoints, List<Pod>> backend, List<ServiceInstance> previousInstances) {
        List<ServiceInstance> serviceInstances = new ArrayList();

        for(Map.Entry<Endpoints, List<Pod>> entry : backend.entrySet()) {
            Endpoints endPoints = (Endpoints)entry.getKey();
            List<Pod> pods = (List)entry.getValue();

            for(EndpointSubset subset : endPoints.getSubsets()) {
                for(EndpointAddress endpointAddress : subset.getAddresses()) {
                    String podName = endpointAddress.getTargetRef().getName();
                    String hostname = endpointAddress.getIp();
                    if (hostname == null) {
                        hostname = endpointAddress.getHostname();
                    }

                    List<EndpointPort> endpointPorts = subset.getPorts();
                    Integer port = 0;
                    String protocol = "";
                    if (endpointPorts.size() == 1) {
                        port = ((EndpointPort)endpointPorts.get(0)).getPort();
                        protocol = ((EndpointPort)endpointPorts.get(0)).getProtocol();
                    } else {
                        for(EndpointPort endpointPort : endpointPorts) {
                            if (this.portName == null || this.portName.equals(endpointPort.getName())) {
                                port = endpointPort.getPort();
                                protocol = endpointPort.getProtocol();
                                break;
                            }
                        }
                    }

                    ServiceInstance matching = ServiceInstanceUtils.findMatching(previousInstances, hostname, port);
                    if (matching != null) {
                        serviceInstances.add(matching);
                    } else {
                        Map<String, String> labels = new HashMap(endPoints.getMetadata().getLabels() != null ? endPoints.getMetadata().getLabels() : Collections.emptyMap());
                        Optional<Pod> maybePod = pods.stream().filter((pod) -> pod.getMetadata().getName().equals(podName)).findFirst();
                        String podNamespace = this.namespace;
                        if (maybePod.isPresent()) {
                            Pod pod = (Pod)maybePod.get();
                            ObjectMeta metadata = pod.getMetadata();
                            podNamespace = metadata.getNamespace();
                            Map<String, String> podLabels = metadata.getLabels();

                            for(Map.Entry<String, String> label : podLabels.entrySet()) {
                                labels.putIfAbsent((String)label.getKey(), (String)label.getValue());
                            }
                        }

                        Metadata<KubernetesMetadataKey> k8sMetadata = Metadata.of(KubernetesMetadataKey.class);
                        serviceInstances.add(new DefaultServiceInstance(ServiceInstanceIds.next(), hostname, port, Optional.empty(), this.secure, labels, k8sMetadata.with(KubernetesMetadataKey.META_K8S_SERVICE_ID, hostname).with(KubernetesMetadataKey.META_K8S_NAMESPACE, podNamespace).with(KubernetesMetadataKey.META_K8S_PORT_PROTOCOL, protocol)));
                    }
                }
            }
        }

        return serviceInstances;
    }

    private static boolean isSecure(KubernetesConfiguration config) {
        return config.getSecure() != null && Boolean.parseBoolean(config.getSecure());
    }
}
