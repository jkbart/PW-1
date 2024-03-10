package cp2023.solution;

import cp2023.base.ComponentId;
import cp2023.base.ComponentTransfer;
import cp2023.base.DeviceId;
import cp2023.base.StorageSystem;
import cp2023.exceptions.*;

import java.util.*;
import java.util.concurrent.*;

public class StorageSystemImpl implements StorageSystem {
    private final ConcurrentMap<DeviceId, Device> devices = new ConcurrentHashMap<>();
    private final ConcurrentMap<ComponentId, Component> components =  new ConcurrentHashMap<>();

    public StorageSystemImpl(
            Map<DeviceId, Integer> deviceTotalSlots,
            Map<ComponentId, DeviceId> componentPlacement) {

        if (deviceTotalSlots == null || componentPlacement == null)
            throw new IllegalArgumentException("Attempt at creating StorageSystem with null argument.");

        Map<DeviceId, Queue<Component>> devicesComponents = new HashMap<>();

        for (var entry : componentPlacement.entrySet()) {
            if (entry.getValue() == null || entry.getKey() == null)
                throw new IllegalArgumentException("Attempt at creating StorageSystem with null argument.");

            Queue<Component> q = devicesComponents.get(entry.getValue());
            Component c = new Component(entry.getKey(), null);
            if (q == null)
                devicesComponents.put(
                        entry.getValue(),
                        new LinkedList<>(
                                List.of(c)));
            else
                q.add(c);

            if(components.put(entry.getKey(), c) != null)
                throw new IllegalArgumentException("Attempt at creating StorageSystem with duplicate component.");
        }

        for (var entry : deviceTotalSlots.entrySet()) {
            if (entry.getValue() == null || entry.getKey() == null)
                throw new IllegalArgumentException("Attempt at creating StorageSystem with null argument.");

            Queue<Component> q = devicesComponents.remove(entry.getKey());
            Device d;

            if (q == null)
                q = new LinkedList<>();

            d = new Device(
                    entry.getKey(),
                    entry.getValue(),
                    q.size());

            devices.put(entry.getKey(),d);

            for (var entry2 : q) {
                entry2.setCurrentDevice(d);
            }
            if (entry.getValue() == 0)
                throw new IllegalArgumentException("Device " + entry.getKey() + " with size 0 is not allowed.");
            if (entry.getValue() < q.size())
                throw new IllegalArgumentException("Device " + entry.getKey() + " has more components than size.");
        }

        if (!devicesComponents.isEmpty())
            throw new IllegalArgumentException("Attempt at creating StorageSystem with device without declared size.");
    }

    private final Semaphore transferBeginProcedureLock = new Semaphore(1, true);

    public void execute(ComponentTransfer transfer) throws TransferException {
        try {
            transferBeginProcedureLock.acquire();

            // Setting up helpful variables.
            DeviceId sourceDeviceId = transfer.getSourceDeviceId();
            DeviceId destinationDeviceId = transfer.getDestinationDeviceId();

            Device sourceDevice = null;
            if (sourceDeviceId != null)
                sourceDevice = devices.get(sourceDeviceId);

            Device destinationDevice = null;
            if (destinationDeviceId != null)
                destinationDevice = devices.get(destinationDeviceId);

            ComponentId transferredComponentId = transfer.getComponentId();
            Component transferredComponent = components.get(transferredComponentId);

            try {
                // Check for ComponentIsBeingOperatedOn
                if (transferredComponent != null && transferredComponent.isTransferred())
                    throw new ComponentIsBeingOperatedOn(transferredComponentId);

                // Check for IllegalTransferType.
                if (sourceDeviceId == null && destinationDeviceId == null)
                    throw new IllegalTransferType(transferredComponentId);

                // Check for DeviceDoesNotExists.
                if (sourceDeviceId != null && sourceDevice == null)
                    throw new DeviceDoesNotExist(sourceDeviceId);

                if (destinationDeviceId != null && destinationDevice == null)
                    throw new DeviceDoesNotExist(destinationDeviceId);

                // Check for ComponentAlreadyExists, assuming component exists on source device until end of transfer prepare.
                {
                    Device d;
                    if (sourceDeviceId == null && transferredComponent != null) {
                        d = transferredComponent.getCurrentDevice();
                        if (d != null)
                            throw new ComponentAlreadyExists(transferredComponentId, d.getId());
                        else
                            throw new ComponentAlreadyExists(transferredComponentId);
                    }
                }

                // Check for ComponentDoesNotExist.
                {
                    DeviceId currentDeviceId = null;

                    if (transferredComponent != null) {
                        Device d = transferredComponent.getCurrentDevice();
                        if (d != null)
                            currentDeviceId = d.getId();
                    }

                    if (sourceDeviceId != null && !sourceDeviceId.equals(currentDeviceId))
                        throw new ComponentDoesNotExist(transferredComponentId, sourceDeviceId);
                }

                // Check for ComponentDoesNotNeedTransfer.
                // We know, that component is not operated on, so currentDevice will not change during this check.
                {
                    DeviceId currentDeviceId = null;
                    if (transferredComponent != null && destinationDeviceId != null) {
                        Device d = transferredComponent.getCurrentDevice();
                        if (d != null)
                            currentDeviceId = d.getId();
                        if (destinationDeviceId.equals(currentDeviceId))
                            throw new ComponentDoesNotNeedTransfer(transferredComponentId, destinationDeviceId);
                    }
                }

            } catch (Exception e) {
                transferBeginProcedureLock.release();
                throw e;
            }

            // In case when we add new component.
            if (transferredComponent == null) {
                transferredComponent = new Component(transferredComponentId, null);
                components.put(transferredComponentId, transferredComponent);
            }

            transferredComponent.setTransfer();
            // Handling operation of adding component.
            if (sourceDevice == null) {
                destinationDevice.reserveSpot(transferredComponent);
            } else if (destinationDevice == null) {
                sourceDevice.acquireAccess();
                
                // We can start removing immediately.
                transferredComponent.getWakeCallToPrepare().release();
                transferredComponent.getWakeCallToPerform().release();

                sourceDevice.releaseAccess();
            } else {
                if (!destinationDevice.reserveSpot(transferredComponent)) {
                    // Try to find cycle of transfers.
                    LinkedList<Component> cycle = new LinkedList<Component>();
                    
                    if (Device.findCycle(sourceDevice, new HashSet<>(), cycle)) {
                        // Cycle was found.
                        Component last = cycle.peekLast();

                        for (Component entry : cycle) {
                            last.setNextToPerform(entry);
                            last = entry;
                        }
                        
                        for (Component entry : cycle) {
                            entry.getWakeCallToPrepare().release();
                        }
                    }
                }
            }
            
            transferBeginProcedureLock.release();

            transferredComponent.getWakeCallToPrepare().acquire();

            if (sourceDevice != null) {
                sourceDevice.acquireAccess();

                //  Picking up transfer that can be prepared next.
                if (transferredComponent.getNextToPerform() == null)
                    transferredComponent.setNextToPerform(sourceDevice.getPrepareWaitLineIn().poll());

                // Waking up transfer that reserved space after component moved in this transfer.
                if (transferredComponent.getNextToPerform() != null)
                    transferredComponent.getNextToPerform().getWakeCallToPrepare().release();

                // If we didn't pick up any transfer, then leaving sign that one incoming transfer can be prepared.
                if (transferredComponent.getNextToPerform() == null)
                    sourceDevice.getPerformWaitSetOut().add(transferredComponent);

                sourceDevice.releaseAccess();
            }

            transfer.prepare();

            if (sourceDevice != null) {
                sourceDevice.acquireAccess();

                // Removing option for left transfers to reserve space after transferred component.
                sourceDevice.getPerformWaitSetOut().remove(transferredComponent);
                
                // Waking up transfer that reserved space after component moved in this transfer or releasing one spot on source device.
                if (transferredComponent.getNextToPerform() != null)
                    transferredComponent.getNextToPerform().getWakeCallToPerform().release();
                else
                    sourceDevice.chgUsedSpotsCnt(-1);

                sourceDevice.releaseAccess();
            }

            transferredComponent.getWakeCallToPerform().acquire();

            // No need to change anything on destinationDevice,
            // since component is moving in space left after some other component.

            transfer.perform();

            transferredComponent.finishTransfer(destinationDevice);

        } catch (InterruptedException e) {
            // From task assumptions we don't need to worry about InterruptedException.
            throw new RuntimeException("panic: unexpected thread interruption");
        }
    }
    private class Component {
        private final ComponentId id;
        private Device currentDevice;
        private volatile Boolean isTransferred = false;
        private Semaphore wakeCallToPrepare = new Semaphore(0, true);
        private Semaphore wakeCallToPerform = new Semaphore(0, true);
        // Component that reserved space after this component (only used during transfer).
        private Component nextToPerform = null;
        public Component(ComponentId id, Device currentDevice) {
            this.id = id;
            this.currentDevice = currentDevice;
        }

        public ComponentId getId() {
            return id;
        }

        public Device getCurrentDevice() {
            return currentDevice;
        }

        public void setCurrentDevice(Device currentDevice) {
            this.currentDevice = currentDevice;
        }

        public Semaphore getWakeCallToPrepare() {
            return wakeCallToPrepare;
        }

        public Semaphore getWakeCallToPerform() {
            return wakeCallToPerform;
        }

        public void setNextToPerform(Component nextToPerform) {
            this.nextToPerform = nextToPerform;
        }

        public Component getNextToPerform() {
            return nextToPerform;
        }

        public void setTransfer() {
            isTransferred = true;
        }
        
        public Boolean isTransferred() {
            return isTransferred;
        }
        
        public void finishTransfer(Device destinationDevice) {
            nextToPerform = null;
            wakeCallToPrepare = new Semaphore(0, true);
            wakeCallToPerform = new Semaphore(0, true);
            if (destinationDevice == null)
                components.remove(id);
            currentDevice = destinationDevice;
            isTransferred = false;
        }
        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof Component)) {
                return false;
            }
            return this.id == ((Component)obj).id;
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }
    }

    // acquireAccess is only required for accessors and getters.
    // All other methods acquire access themselves.
    private class Device {
        private final DeviceId id;
        private final int totalSpots;
        private final Semaphore access = new Semaphore(1, true);
        private int usedSpotsCnt;
        // Chronological queue of components waiting for permission to move to this device.
        private Queue<Component> prepareWaitLineIn = new LinkedList<Component>();
        // Set of transferred components currently on this device that have permission to execute perform and no one reserved spot after them.
        private Set<Component> performWaitSetOut = new HashSet<>();

        public Device(DeviceId id, int totalSpots, int usedSpotsCnt) {
            this.id = id;
            this.totalSpots = totalSpots;
            this.usedSpotsCnt = usedSpotsCnt;
        }

        public void acquireAccess() {
            try {
                access.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException("panic: unexpected thread interruption");
            }
        }

        public void releaseAccess() {
            access.release();
        }

        public DeviceId getId() {
            return id;
        }

        public int getTotalSpots() {
            return totalSpots;
        }

        public int getUsedSpotsCnt() {
            return usedSpotsCnt;
        }
        public void chgUsedSpotsCnt(int inc) {
            usedSpotsCnt += inc;
        }

        public Queue<Component> getPrepareWaitLineIn() {
            return prepareWaitLineIn;
        }

        public Set<Component> getPerformWaitSetOut() {
            return performWaitSetOut;
        }

        // Returns true if spot was reserved, otherwise adds component to prepareWaitLineIn and returns false.
        public Boolean reserveSpot(Component transferredComponent) {
            this.acquireAccess();

            try {
                if (this.getUsedSpotsCnt() < this.getTotalSpots()) {
                    this.chgUsedSpotsCnt(1);

                    // There is space on the device, so we can prepare and perform immediately.
                    transferredComponent.getWakeCallToPrepare().release();
                    transferredComponent.getWakeCallToPerform().release();

                    return true;
                } else if (!this.getPerformWaitSetOut().isEmpty()) {
                    // There is spot we can reserve, so we can prepare immediately.
                    transferredComponent.getWakeCallToPrepare().release();

                    Iterator<Component> iter = this.getPerformWaitSetOut().iterator();
                    iter.next().setNextToPerform(transferredComponent);
                    iter.remove();

                    return true;
                } else {
                    this.getPrepareWaitLineIn().add(transferredComponent);

                    return false;
                }
            } finally {
                this.releaseAccess();
            }
        }

        // Defines edges from device v_1, as pairs (v_1, v_2) where v_2 is c.currentDevice() for some c in v_1.getPrepareWaitLineIn().
        // Returns true if path was found from v to any device from seen or v.
        // Sets currentPath to founded path or cycle.
        // Removes components from path from their prepareWaitLine.
        public static Boolean findCycle(Device v, Set<DeviceId> seen, LinkedList<Component> currentPath) {
            v.acquireAccess();

            try {
                seen.add(v.getId());
                Iterator<Component> iter = v.getPrepareWaitLineIn().iterator();

                while (iter.hasNext()) {
                    Component x = iter.next();
                    Device xd = x.getCurrentDevice();

                    if (xd == null)
                        continue;

                    currentPath.addLast(x);
                    if (seen.contains(xd.getId())) {
                        // We can assume that currentPath is a cycle since we are calling this function at the beginning of every transfer.
                        iter.remove();
                        return true;
                    }

                    if (findCycle(xd, seen, currentPath)) {
                        iter.remove();
                        return true;
                    }

                    currentPath.pollLast();
                }
                seen.remove(v.getId());

                return false;
            } finally {
                v.releaseAccess();
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (! (obj instanceof Device)) {
                return false;
            }
            return this.id.equals(((Device)obj).id);
        }

        @Override
        public int hashCode() {
            return this.id.hashCode();
        }
    }
}