package org.terasology.signalling.componentSystem;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terasology.blockNetwork.BlockNetwork;
import org.terasology.blockNetwork.Network;
import org.terasology.blockNetwork.NetworkNode;
import org.terasology.blockNetwork.SidedLocationNetworkNode;
import org.terasology.blockNetwork.NetworkTopologyListener;
import org.terasology.engine.Time;
import org.terasology.entitySystem.entity.EntityRef;
import org.terasology.entitySystem.entity.lifecycleEvents.BeforeDeactivateComponent;
import org.terasology.entitySystem.entity.lifecycleEvents.OnActivatedComponent;
import org.terasology.entitySystem.entity.lifecycleEvents.OnChangedComponent;
import org.terasology.entitySystem.event.ReceiveEvent;
import org.terasology.entitySystem.systems.BaseComponentSystem;
import org.terasology.entitySystem.systems.RegisterMode;
import org.terasology.entitySystem.systems.RegisterSystem;
import org.terasology.entitySystem.systems.UpdateSubscriberSystem;
import org.terasology.math.Side;
import org.terasology.math.SideBitFlag;
import org.terasology.math.Vector3i;
import org.terasology.registry.In;
import org.terasology.signalling.components.SignalConductorComponent;
import org.terasology.signalling.components.SignalConsumerAdvancedStatusComponent;
import org.terasology.signalling.components.SignalConsumerComponent;
import org.terasology.signalling.components.SignalConsumerStatusComponent;
import org.terasology.signalling.components.SignalProducerComponent;
import org.terasology.world.BlockEntityRegistry;
import org.terasology.world.WorldProvider;
import org.terasology.world.block.BeforeDeactivateBlocks;
import org.terasology.world.block.BlockComponent;
import org.terasology.world.block.OnActivatedBlocks;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

@RegisterSystem(value = RegisterMode.AUTHORITY)
public class SignalSystem extends BaseComponentSystem implements UpdateSubscriberSystem, NetworkTopologyListener {
    private static final Logger logger = LoggerFactory.getLogger(SignalSystem.class);

    @In
    private Time time;
    @In
    private WorldProvider worldProvider;

    @In
    private BlockEntityRegistry blockEntityRegistry;

    private BlockNetwork signalNetwork;

    // we assume there can be only one consumer, one producer, and/or one conductor per block
    private Map<Vector3i, SidedLocationNetworkNode> signalProducers;
    private Map<Vector3i, SidedLocationNetworkNode> signalConsumers;
    private Map<Vector3i, SidedLocationNetworkNode> signalConductors;

    private Multimap<SidedLocationNetworkNode, Network> producerNetworks = HashMultimap.create();
    private Multimap<Network, SidedLocationNetworkNode> producersInNetwork = HashMultimap.create();

    private Multimap<SidedLocationNetworkNode, Network> consumerNetworks = HashMultimap.create();
    private Multimap<Network, SidedLocationNetworkNode> consumersInNetwork = HashMultimap.create();

    // Used to detect producer changes
    private Map<SidedLocationNetworkNode, Integer> producerSignalStrengths = Maps.newHashMap();
    // Used to store signal for consumer from non-modified networks
    private Map<SidedLocationNetworkNode, Map<Network, NetworkSignals>> consumerSignalInNetworks = Maps.newHashMap();

    private Set<Network> networksToRecalculate = Sets.newHashSet();
    private Set<SidedLocationNetworkNode> consumersToRecalculate = Sets.newHashSet();
    private Set<SidedLocationNetworkNode> producersSignalsChanged = Sets.newHashSet();

    private class NetworkSignals {
        private byte sidesWithSignal;
        private byte sidesWithoutSignal;

        private NetworkSignals(byte sidesWithSignal, byte sidesWithoutSignal) {
            this.sidesWithSignal = sidesWithSignal;
            this.sidesWithoutSignal = sidesWithoutSignal;
        }
    }

    @Override
    public void initialise() {
        signalNetwork = new BlockNetwork();
        signalNetwork.addTopologyListener(this);
        signalProducers = Maps.newHashMap();
        signalConsumers = Maps.newHashMap();
        signalConductors = Maps.newHashMap();
        logger.info("Initialized SignalSystem");
    }

    @Override
    public void shutdown() {
        signalNetwork = null;
        signalProducers = null;
        signalConsumers = null;
        signalConductors = null;
    }

    private long lastUpdate;
    private static final long PROCESSING_MINIMUM_INTERVAL = 0;
    private static final boolean CONSUMER_CAN_POWER_ITSELF = true;

    @Override
    public void update(float delta) {
        long worldTime = time.getGameTimeInMs();
        if (worldTime > lastUpdate + PROCESSING_MINIMUM_INTERVAL) {
            lastUpdate = worldTime;

            // Mark all networks affected by the producer signal change
            for (SidedLocationNetworkNode producerChanges : producersSignalsChanged) {
                networksToRecalculate.addAll(producerNetworks.get(producerChanges));
            }

            Set<SidedLocationNetworkNode> consumersToEvaluate = Sets.newHashSet();

            for (Network network : networksToRecalculate) {
                if (signalNetwork.isNetworkActive(network)) {
                    Collection<SidedLocationNetworkNode> consumers = this.consumersInNetwork.get(network);
                    for (SidedLocationNetworkNode consumerLocation : consumers) {
                        NetworkSignals consumerSignalInNetwork = getConsumerSignalInNetwork(network, consumerLocation);
                        consumerSignalInNetworks.get(consumerLocation).put(network, consumerSignalInNetwork);
                    }
                    consumersToEvaluate.addAll(consumers);
                }
            }

            for (SidedLocationNetworkNode modifiedConsumer : consumersToRecalculate) {
                Collection<Network> networks = consumerNetworks.get(modifiedConsumer);
                for (Network network : networks) {
                    NetworkSignals consumerSignalInNetwork = getConsumerSignalInNetwork(network, modifiedConsumer);
                    consumerSignalInNetworks.get(modifiedConsumer).put(network, consumerSignalInNetwork);
                }
                consumersToEvaluate.add(modifiedConsumer);
            }

            // Clearing the changed states
            producersSignalsChanged.clear();
            networksToRecalculate.clear();
            consumersToRecalculate.clear();

            // Send consumer status changes
            for (SidedLocationNetworkNode consumerToEvaluate : consumersToEvaluate) {
                if (signalConsumers.containsValue(consumerToEvaluate)) {
                    final EntityRef blockEntity = blockEntityRegistry.getBlockEntityAt(consumerToEvaluate.location);
                    final SignalConsumerComponent consumerComponent = blockEntity.getComponent(SignalConsumerComponent.class);
                    if (consumerComponent != null) {
                        Map<Network, NetworkSignals> consumerSignals = consumerSignalInNetworks.get(consumerToEvaluate);
                        processSignalConsumerResult(consumerSignals.values(), consumerComponent, blockEntity);
                    }
                }
            }
        }
    }

    private void processSignalConsumerResult(Collection<NetworkSignals> networkSignals, SignalConsumerComponent signalConsumerComponent, EntityRef entity) {
        final SignalConsumerComponent.Mode mode = signalConsumerComponent.mode;
        switch (mode) {
            // OR
            case AT_LEAST_ONE: {
                final boolean signal = hasSignalForOr(networkSignals);
                outputSignalToSimpleConsumer(entity, signal);
                return;
            }
            // AND
            case ALL_CONNECTED: {
                final boolean signal = hasSignalForAnd(networkSignals);
                outputSignalToSimpleConsumer(entity, signal);
                return;
            }
            // XOR
            case EXACTLY_ONE: {
                final boolean signal = hasSignalForXor(networkSignals);
                outputSignalToSimpleConsumer(entity, signal);
                return;
            }
            // Special leaving the calculation to the block's system itself
            case SPECIAL: {
                outputSignalToAdvancedConsumer(entity, networkSignals);
                return;
            }
            default:
                throw new IllegalArgumentException("Unknown mode set for SignalConsumerComponent");
        }
    }

    private void outputSignalToAdvancedConsumer(EntityRef entity, Collection<NetworkSignals> networkSignals) {
        final SignalConsumerAdvancedStatusComponent advancedStatusComponent = entity.getComponent(SignalConsumerAdvancedStatusComponent.class);
        byte withoutSignal = 0;
        byte withSignal = 0;
        if (networkSignals != null) {
            for (NetworkSignals networkSignal : networkSignals) {
                withoutSignal |= networkSignal.sidesWithoutSignal;
                withSignal |= networkSignal.sidesWithSignal;
            }
        }
        if (advancedStatusComponent.sidesWithoutSignals != withoutSignal
                || advancedStatusComponent.sidesWithSignals != withSignal) {
            advancedStatusComponent.sidesWithoutSignals = withoutSignal;
            advancedStatusComponent.sidesWithSignals = withSignal;
            entity.saveComponent(advancedStatusComponent);
        }
    }

    private void outputSignalToSimpleConsumer(EntityRef entity, boolean result) {
        final SignalConsumerStatusComponent consumerStatusComponent = entity.getComponent(SignalConsumerStatusComponent.class);
        if (consumerStatusComponent.hasSignal != result) {
            consumerStatusComponent.hasSignal = result;
            entity.saveComponent(consumerStatusComponent);
            logger.debug("Consumer has signal: " + result);
        }
    }

    private boolean hasSignalForXor(Collection<NetworkSignals> networkSignals) {
        if (networkSignals == null) {
            return false;
        }
        boolean connected = false;
        for (NetworkSignals networkSignal : networkSignals) {
            if (SideBitFlag.getSides(networkSignal.sidesWithSignal).size() > 1) {
                // More than one side connected in network
                return false;
            } else if (networkSignal.sidesWithSignal > 0) {
                if (connected) {
                    // One side connected in network, but already connected in other network
                    return false;
                } else {
                    connected = true;
                }
            }
        }

        return connected;
    }

    private boolean hasSignalForAnd(Collection<NetworkSignals> networkSignals) {
        if (networkSignals == null) {
            return false;
        }
        for (NetworkSignals networkSignal : networkSignals) {
            if (networkSignal.sidesWithoutSignal > 0) {
                return false;
            }
        }
        return true;
    }

    private boolean hasSignalForOr(Collection<NetworkSignals> networkSignals) {
        if (networkSignals == null) {
            return false;
        }
        for (NetworkSignals networkSignal : networkSignals) {
            if (networkSignal.sidesWithSignal > 0) {
                return true;
            }
        }
        return false;
    }

    //
    private NetworkSignals getConsumerSignalInNetwork(Network network, SidedLocationNetworkNode consumerNode) {
        // Check for infinite signal strength (-1), if there - it powers whole network
        final Collection<SidedLocationNetworkNode> producers = producersInNetwork.get(network);
        for (SidedLocationNetworkNode producer : producers) {
            if (CONSUMER_CAN_POWER_ITSELF || !producer.location.equals(consumerNode.location)) {
                final int signalStrength = producerSignalStrengths.get(producer);
                if (signalStrength == -1) {
                    return new NetworkSignals(getLeafSidesInNetwork(consumerNode), (byte) 0);
                }
            }
        }

        byte sidesInNetwork = getLeafSidesInNetwork(consumerNode);
        byte sidesWithSignal = 0;

        for (Side sideInNetwork : SideBitFlag.getSides(sidesInNetwork)) {
            if (hasSignalInNetworkOnSide(network, consumerNode, producers, sideInNetwork)) {
                sidesWithSignal += SideBitFlag.getSide(sideInNetwork);
                break;
            }
        }

        return new NetworkSignals(sidesWithSignal, (byte) (sidesInNetwork - sidesWithSignal));
    }

    private byte getLeafSidesInNetwork(SidedLocationNetworkNode networkNode) {
        Iterable<NetworkNode> connectedNodes = signalNetwork.getAdjacentNodes(networkNode);
        Set<Side> connectedSides = Sets.newHashSet();
        for(NetworkNode connectedNode : connectedNodes ) {
            connectedSides.add(networkNode.connectionSide((SidedLocationNetworkNode) connectedNode));
        }

        return SideBitFlag.getSides(connectedSides);
    }


    private boolean hasSignalInNetworkOnSide(Network network, SidedLocationNetworkNode consumerNode, Collection<SidedLocationNetworkNode> producers, Side sideInNetwork) {
        for (SidedLocationNetworkNode producer : producers) {
            if (CONSUMER_CAN_POWER_ITSELF || !producer.location.equals(consumerNode.location)) {
                final int signalStrength = producerSignalStrengths.get(producer);
                if (isInDistanceWithSide(signalStrength, producer, consumerNode, sideInNetwork)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isInDistanceWithSide(int signalStrength, SidedLocationNetworkNode producer, SidedLocationNetworkNode consumer, Side sideInNetwork) {
        return signalNetwork.isInDistance(signalStrength, producer, consumer, SidedLocationNetworkNode.createSideConnectivityFilter(sideInNetwork, consumer.location));
    }

    @Override
    public void networkAdded(Network newNetwork) {
    }

    @Override
    public void networkingNodeAdded(Network network, NetworkNode networkingNode) {
        logger.debug("Cable added to network");
        SignalNetworkNode modifiedLeafNode = (SignalNetworkNode) networkingNode;

        if ( modifiedLeafNode.getType() == SignalNetworkNode.Type.PRODUCER) {
            logger.debug("Producer added to network");
            networksToRecalculate.add(network);
            producerNetworks.put(modifiedLeafNode, network);
            producersInNetwork.put(network, modifiedLeafNode);
        } else         if ( modifiedLeafNode.getType() == SignalNetworkNode.Type.CONSUMER ) {

            logger.debug("Consumer added to network");
            consumersToRecalculate.add(modifiedLeafNode);
            consumerNetworks.put(modifiedLeafNode, network);
            consumersInNetwork.put(network, modifiedLeafNode);
        }          else {
            networksToRecalculate.add(network);
        }
    }

    @Override
    public void networkingNodeRemoved(Network network, NetworkNode networkingNode) {
        logger.debug("Cable removed from network");
        SignalNetworkNode modifiedLeafNode = (SignalNetworkNode) networkingNode;

        if (((SignalNetworkNode) modifiedLeafNode).getType() == SignalNetworkNode.Type.PRODUCER) {
            logger.debug("Producer removed from network");
            networksToRecalculate.add(network);
            producerNetworks.remove(modifiedLeafNode, network);
            producersInNetwork.remove(network, modifiedLeafNode);
        } else         if ( modifiedLeafNode.getType() == SignalNetworkNode.Type.CONSUMER ) {
            logger.debug("Consumer removed from network");
            consumersToRecalculate.add(modifiedLeafNode);
            consumerNetworks.remove(modifiedLeafNode, network);
            consumersInNetwork.remove(network, modifiedLeafNode);
            consumerSignalInNetworks.get(modifiedLeafNode).remove(network);
        }          else {
            networksToRecalculate.add(network);
        }
    }


    @Override
    public void networkRemoved(Network network) {
    }

    private SignalNetworkNode toNode(Vector3i location, byte directions, SignalNetworkNode.Type type) {
        return new SignalNetworkNode(location, directions, type);
    }

    /*
     * ****************************** Conductor events ********************************
     */

    @ReceiveEvent(components = {SignalConductorComponent.class})
    public void prefabConductorLoaded(OnActivatedBlocks event, EntityRef blockType) {
        byte connectingOnSides = blockType.getComponent(SignalConductorComponent.class).connectionSides;
        Set<SidedLocationNetworkNode> conductorNodes = Sets.newHashSet();
        for (Vector3i location : event.getBlockPositions()) {
            final SidedLocationNetworkNode conductorNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.CONDUCTOR);
            conductorNodes.add(conductorNode);

            signalConductors.put(conductorNode.location, conductorNode);
            signalNetwork.addNetworkingBlock(conductorNode);
        }

    }

    @ReceiveEvent(components = {SignalConductorComponent.class})
    public void prefabConductorUnloaded(BeforeDeactivateBlocks event, EntityRef blockType) {
        byte connectingOnSides = blockType.getComponent(SignalConductorComponent.class).connectionSides;
        Set<SidedLocationNetworkNode> conductorNodes = Sets.newHashSet();
        // Quite messy due to the order of operations, need to check if the order is important
        for (Vector3i location : event.getBlockPositions()) {
            final SidedLocationNetworkNode conductorNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.CONDUCTOR);
            conductorNodes.add(conductorNode);
        }
        for (SidedLocationNetworkNode conductorNode : conductorNodes) {
            signalNetwork.removeNetworkingBlock(conductorNode);
            signalConductors.remove(conductorNode.location);
        }
    }

    @ReceiveEvent(components = {BlockComponent.class, SignalConductorComponent.class})
    public void conductorAdded(OnActivatedComponent event, EntityRef block) {
        byte connectingOnSides = block.getComponent(SignalConductorComponent.class).connectionSides;

        final Vector3i location = new Vector3i(block.getComponent(BlockComponent.class).getPosition());
        final SidedLocationNetworkNode conductorNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.CONDUCTOR);

        signalConductors.put(conductorNode.location, conductorNode);
        signalNetwork.addNetworkingBlock(conductorNode);
    }

    @ReceiveEvent(components = {SignalConductorComponent.class})
    public void conductorUpdated(OnChangedComponent event, EntityRef block) {
        if (block.hasComponent(BlockComponent.class)) {
            byte connectingOnSides = block.getComponent(SignalConductorComponent.class).connectionSides;

            final Vector3i location = new Vector3i(block.getComponent(BlockComponent.class).getPosition());

            final SidedLocationNetworkNode oldConductorNode = signalConductors.get(location);
            if (oldConductorNode != null) {
                final SidedLocationNetworkNode newConductorNode = toNode(new Vector3i(location), connectingOnSides, SignalNetworkNode.Type.CONDUCTOR);
                signalConductors.put(newConductorNode.location, newConductorNode);
                signalNetwork.updateNetworkingBlock(oldConductorNode, newConductorNode);
            }
        }
    }

    @ReceiveEvent(components = {BlockComponent.class, SignalConductorComponent.class})
    public void conductorRemoved(BeforeDeactivateComponent event, EntityRef block) {
        byte connectingOnSides = block.getComponent(SignalConductorComponent.class).connectionSides;

        final Vector3i location = new Vector3i(block.getComponent(BlockComponent.class).getPosition());
        final SidedLocationNetworkNode conductorNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.CONDUCTOR);
        signalNetwork.removeNetworkingBlock(conductorNode);
        signalConductors.remove(conductorNode.location);
    }

    /*
     * ****************************** Producer events ********************************
     */

    @ReceiveEvent(components = {SignalProducerComponent.class})
    public void prefabProducerLoaded(OnActivatedBlocks event, EntityRef blockType) {
        final SignalProducerComponent producerComponent = blockType.getComponent(SignalProducerComponent.class);
        byte connectingOnSides = producerComponent.connectionSides;
        int signalStrength = producerComponent.signalStrength;
        Set<SidedLocationNetworkNode> producerNodes = Sets.newHashSet();
        for (Vector3i location : event.getBlockPositions()) {
            final SidedLocationNetworkNode producerNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.PRODUCER);

            signalProducers.put(producerNode.location, producerNode);
            producerSignalStrengths.put(producerNode, signalStrength);
            producerNodes.add(producerNode);
            signalNetwork.addNetworkingBlock(producerNode);
        }
    }

    @ReceiveEvent(components = {SignalProducerComponent.class})
    public void prefabProducerUnloaded(BeforeDeactivateBlocks event, EntityRef blockType) {
        byte connectingOnSides = blockType.getComponent(SignalProducerComponent.class).connectionSides;
        // Quite messy due to the order of operations, need to check if the order is important
        Set<SidedLocationNetworkNode> producerNodes = Sets.newHashSet();
        for (Vector3i location : event.getBlockPositions()) {
            final SidedLocationNetworkNode producerNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.PRODUCER);
            producerNodes.add(producerNode);
        }

        for (SidedLocationNetworkNode producerNode : producerNodes) {
            signalNetwork.removeNetworkingBlock(producerNode);
            signalProducers.remove(producerNode.location);
            producerSignalStrengths.remove(producerNode);
        }
    }

    @ReceiveEvent(components = {BlockComponent.class, SignalProducerComponent.class})
    public void producerAdded(OnActivatedComponent event, EntityRef block) {
        Vector3i location = new Vector3i(block.getComponent(BlockComponent.class).getPosition());
        final SignalProducerComponent producerComponent = block.getComponent(SignalProducerComponent.class);
        final int signalStrength = producerComponent.signalStrength;
        byte connectingOnSides = producerComponent.connectionSides;

        final SidedLocationNetworkNode producerNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.PRODUCER);

        signalProducers.put(producerNode.location, producerNode);
        producerSignalStrengths.put(producerNode, signalStrength);
        signalNetwork.addNetworkingBlock(producerNode);
    }

    @ReceiveEvent(components = {SignalProducerComponent.class})
    public void producerUpdated(OnChangedComponent event, EntityRef block) {
        logger.debug("Producer updated: " + block.getParentPrefab());
        if (block.hasComponent(BlockComponent.class)) {
            Vector3i location = new Vector3i(block.getComponent(BlockComponent.class).getPosition());
            final SignalProducerComponent producerComponent = block.getComponent(SignalProducerComponent.class);

            // We need to figure out, what exactly was changed
            final byte oldConnectionSides = signalProducers.get(location).connectionSides;
            byte newConnectionSides = producerComponent.connectionSides;

            SidedLocationNetworkNode node = toNode(location, newConnectionSides, SignalNetworkNode.Type.PRODUCER);
            SidedLocationNetworkNode oldNode = toNode(location, oldConnectionSides, SignalNetworkNode.Type.PRODUCER);
            if (oldConnectionSides != newConnectionSides) {
                producerSignalStrengths.put(node, producerComponent.signalStrength);
                signalProducers.put(node.location, node);
                signalNetwork.updateNetworkingBlock(oldNode, node);
                producerSignalStrengths.remove(oldNode);
            } else {
                int oldSignalStrength = producerSignalStrengths.get(oldNode);
                int newSignalStrength = producerComponent.signalStrength;
                if (oldSignalStrength != newSignalStrength) {
                    producerSignalStrengths.put(node, newSignalStrength);
                    producersSignalsChanged.add(node);
                }
            }
        }
    }

    @ReceiveEvent(components = {BlockComponent.class, SignalProducerComponent.class})
    public void producerRemoved(BeforeDeactivateComponent event, EntityRef block) {
        Vector3i location = new Vector3i(block.getComponent(BlockComponent.class).getPosition());
        byte connectingOnSides = block.getComponent(SignalProducerComponent.class).connectionSides;

        final SidedLocationNetworkNode producerNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.PRODUCER);
        signalNetwork.removeNetworkingBlock(producerNode);
        signalProducers.remove(producerNode.location);
        producerSignalStrengths.remove(producerNode);
    }

    /*
     * ****************************** Consumer events ********************************
     */

    @ReceiveEvent(components = {SignalConsumerComponent.class})
    public void prefabConsumerLoaded(OnActivatedBlocks event, EntityRef blockType) {
        byte connectingOnSides = blockType.getComponent(SignalConsumerComponent.class).connectionSides;
        Set<SidedLocationNetworkNode> consumerNodes = Sets.newHashSet();
        for (Vector3i location : event.getBlockPositions()) {
            SidedLocationNetworkNode consumerNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.CONSUMER);

            signalConsumers.put(consumerNode.location, consumerNode);
            consumerSignalInNetworks.put(consumerNode, Maps.<Network, NetworkSignals>newHashMap());
            consumerNodes.add(consumerNode);
            signalNetwork.addNetworkingBlock(consumerNode);
        }
    }

    @ReceiveEvent(components = {SignalConsumerComponent.class})
    public void prefabConsumerUnloaded(BeforeDeactivateBlocks event, EntityRef blockType) {
        byte connectingOnSides = blockType.getComponent(SignalConsumerComponent.class).connectionSides;
        Set<SidedLocationNetworkNode> consumerNodes = Sets.newHashSet();

        // Quite messy due to the order of operations, need to check if the order is important
        for (Vector3i location : event.getBlockPositions()) {
            SidedLocationNetworkNode consumerNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.CONSUMER);
            consumerNodes.add(consumerNode);
        }

        for (SidedLocationNetworkNode consumerNode : consumerNodes) {
            signalNetwork.removeNetworkingBlock(consumerNode);
            signalConsumers.remove(consumerNode.location);
            consumerSignalInNetworks.remove(consumerNode);
        }
    }

    @ReceiveEvent(components = {BlockComponent.class, SignalConsumerComponent.class})
    public void consumerAdded(OnActivatedComponent event, EntityRef block) {
        Vector3i location = new Vector3i(block.getComponent(BlockComponent.class).getPosition());
        byte connectingOnSides = block.getComponent(SignalConsumerComponent.class).connectionSides;

        SidedLocationNetworkNode consumerNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.CONSUMER);

        signalConsumers.put(consumerNode.location, consumerNode);
        consumerSignalInNetworks.put(consumerNode, Maps.<Network, NetworkSignals>newHashMap());
        signalNetwork.addNetworkingBlock(consumerNode);
    }

    @ReceiveEvent(components = {SignalConsumerComponent.class})
    public void consumerUpdated(OnChangedComponent event, EntityRef block) {
        if (block.hasComponent(BlockComponent.class)) {
            Vector3i location = new Vector3i(block.getComponent(BlockComponent.class).getPosition());
            final SignalConsumerComponent consumerComponent = block.getComponent(SignalConsumerComponent.class);

            // We need to figure out, what exactly was changed
            final byte oldConnectionSides = signalConsumers.get(location).connectionSides;
            byte newConnectionSides = consumerComponent.connectionSides;

            SidedLocationNetworkNode node = toNode(location, newConnectionSides, SignalNetworkNode.Type.CONSUMER);
            if (oldConnectionSides != newConnectionSides) {
                signalConsumers.put(node.location, node);
                SignalNetworkNode oldNode = toNode(location, oldConnectionSides, SignalNetworkNode.Type.CONSUMER);
                consumerSignalInNetworks.put(node, Maps.<Network, NetworkSignals>newHashMap());

                signalNetwork.updateNetworkingBlock(oldNode, node);

                consumerSignalInNetworks.remove(oldNode);
            }
            // Mode could have changed
            consumersToRecalculate.add(node);
        }
    }

    @ReceiveEvent(components = {BlockComponent.class, SignalConsumerComponent.class})
    public void consumerRemoved(BeforeDeactivateComponent event, EntityRef block) {
        Vector3i location = new Vector3i(block.getComponent(BlockComponent.class).getPosition());
        byte connectingOnSides = block.getComponent(SignalConsumerComponent.class).connectionSides;

        final SidedLocationNetworkNode consumerNode = toNode(location, connectingOnSides, SignalNetworkNode.Type.CONSUMER);
        signalNetwork.removeNetworkingBlock(consumerNode);
        signalConsumers.remove(consumerNode.location);
        consumerSignalInNetworks.remove(consumerNode);
    }

}