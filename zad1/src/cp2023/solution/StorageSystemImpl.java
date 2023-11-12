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
    // Set backed by ConcurrentHashMap, because there is no ConcurrentSet in JAVA.
    private final ConcurrentMap<ComponentId, Component> components =  new ConcurrentHashMap<>();
    // FIXME: clone maps, check args
    public StorageSystemImpl(
            Map<DeviceId, Integer> deviceTotalSlots,
            Map<ComponentId, DeviceId> componentPlacement) {
        Map<DeviceId, Queue<Component>> devicesComponents = new ConcurrentHashMap<>();

        for (var entry : componentPlacement.entrySet()) {
            Queue<Component> q = devicesComponents.get(entry.getValue());
            Component c = new Component(entry.getKey(), null);
            if (q == null)
                devicesComponents.put(
                        entry.getValue(),
                        new LinkedList<>(
                                List.of(c)));
            else
                q.add(c);

            System.out.println(entry.getKey() + " + + " + entry.getValue());
            components.put(entry.getKey(), c);
        }

        for (var entry : deviceTotalSlots.entrySet()) {
            Queue<Component> q = devicesComponents.get(entry.getKey());
            Device d;

            if (q == null)
                q = new LinkedList<>();

            d = new Device(
                    entry.getKey(),
                    entry.getValue(),
                    q);
//            devices.put(
//                    entry.getKey(),
//                    new Device(
//                            entry.getKey(),
//                            entry.getValue(),
//                            q));
            devices.put(entry.getKey(),d);

            for (var entry2 : q) {
                entry2.setCurrentDevice(d);
            }
            if (entry.getValue() == 0)
                throw new IllegalArgumentException("Device " + entry.getKey() + " with size 0 is not allowed.");
            if (entry.getValue() < q.size())
                throw new IllegalArgumentException("Device " + entry.getKey() + " has more components than size.");
        }
    }

    private final Semaphore transferBeginProcedureLock = new Semaphore(1, true);

    public void execute(ComponentTransfer transfer) throws TransferException {
    try {
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

        transferBeginProcedureLock.acquire();
        System.out.println("--------Begin procedure BEGAN!!!! for: " + transfer.getSourceDeviceId() + " -(" + transfer.getComponentId() + ")-" + transfer.getDestinationDeviceId());
        System.out.println("StorageSystemStateWaitLines: {");
        for (var entry : devices.entrySet()) {
            System.out.print(entry.getKey() + " -> ");
            for (var entry2 : entry.getValue().getPrepareWaitLineIn())
                System.out.print(entry2.getId());
            System.out.println();
        }
        System.out.println("}");
        System.out.println("StorageSystemComponentsDevices: {");
        for (var entry : components.entrySet()) {
            Device d = entry.getValue().getCurrentDevice();
            System.out.println(entry.getKey() + " -> " + (d == null ? "NULL" : d.getId()));
        }
        System.out.println("}");
        try {

            // Check for ComponentIsBeingOperatedOn
            if (transferredComponent != null && transferredComponent.currentTransfer != null)
                throw new ComponentIsBeingOperatedOn(transferredComponentId);

            // Check for IllegalTransferType.
            if (sourceDeviceId == null &&
                    destinationDeviceId == null)
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
                if (sourceDeviceId != null &&
                         !sourceDeviceId.equals(currentDeviceId))
                    throw new ComponentDoesNotExist(transferredComponentId, sourceDeviceId);
            }

            // Check for ComponentDoesNotNeedTransfer.
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
        System.out.println("Begin procedure finished checking args for: " + transfer.getSourceDeviceId() + " -(" + transfer.getComponentId() + ")-" + transfer.getDestinationDeviceId());
        // In case when we add new component.
        if (transferredComponent == null) {
            transferredComponent = new Component(transferredComponentId, null);
            components.put(transferredComponentId, transferredComponent);
        }

        // FIXME: possibly lock source and destination device.
        // Handling operation of adding component.
        if (sourceDevice == null) {
            destinationDevice.acquireAccess();
            // Check if there is free spot on device.
            if (destinationDevice.getStillCompCnt()
                    < destinationDevice.getTotalSpots()) {
                destinationDevice.chgStillCompCnt(1);
                // There is space on the device, so we can prepare and execute immediately.
                transferredComponent.getWakeCallToPrepare().release();
                transferredComponent.getWakeCallToExecute().release();
            } else {
                if (!destinationDevice.getExecuteWaitSetOut().isEmpty()) {
                    // There is space on the device, so we can prepare and execute immediately.
                    transferredComponent.getWakeCallToPrepare().release();
                    Iterator<Component> iter = destinationDevice.getExecuteWaitSetOut().iterator();
                    iter.next().setNextToTransfer(transferredComponent);
                    iter.remove();
                } else {
                    // We need to wait with prepare for some other transfer.
                    destinationDevice.getPrepareWaitLineIn().add(transferredComponent);
                }
            }
            destinationDevice.releaseAccess();
        }
        else if (destinationDevice == null) {
            sourceDevice.acquireAccess();
            transferredComponent.getWakeCallToPrepare().release();
            transferredComponent.getWakeCallToExecute().release();
            sourceDevice.releaseAccess();
        }
        else {
            destinationDevice.acquireAccess();
            // Check if there is free spot on device.
            if (destinationDevice.getStillCompCnt()
                    < destinationDevice.getTotalSpots()) {
                destinationDevice.chgStillCompCnt(1);
                // There is space on the device, so we can prepare and execute immediately.
                transferredComponent.getWakeCallToPrepare().release();
                transferredComponent.getWakeCallToExecute().release();
            } else {
                if (!destinationDevice.getExecuteWaitSetOut().isEmpty()) {
                    // There is a component on destinationDevice we can reserve space from.
                    transferredComponent.getWakeCallToPrepare().release();
                    Iterator<Component> iter = destinationDevice.getExecuteWaitSetOut().iterator();
                    iter.next().setNextToTransfer(transferredComponent);
                    iter.remove();
                } else {
                    destinationDevice.getPrepareWaitLineIn().add(transferredComponent); // moved
                    System.out.println(destinationDevice.getPrepareWaitLineIn());

                    System.out.println("Begin procedure looking for cycle for: " + transfer.getSourceDeviceId() + " -(" + transfer.getComponentId() + ")-" + transfer.getDestinationDeviceId());
                    // Try to find cycle of transfers.
                    Stack<Component> cycle = new Stack<Component>();
                    if (Device.findCycle(sourceDevice, new HashSet<>(), cycle, destinationDeviceId)) {
                        // Cycle was found.
                        Component start = cycle.pop();
                        start.getWakeCallToPrepare().release();
                        Component last = start;

                        while (!cycle.empty()) {
                            Component current = cycle.pop();
                            current.getWakeCallToPrepare().release();
                            last.setNextToTransfer(current);
                            last = current;
                        }

                        last.setNextToTransfer(start);
                        System.out.println("Begin procedure FOUND cycle for: " + transfer.getSourceDeviceId() + " -(" + transfer.getComponentId() + ")-" + transfer.getDestinationDeviceId());
                    } else {
                        // We need to wait with prepare for some other transfer.
                    }
                    System.out.println("Begin procedure finished looking for cycle for: " + transfer.getSourceDeviceId() + " -(" + transfer.getComponentId() + ")-" + transfer.getDestinationDeviceId());
                }
            }
            destinationDevice.releaseAccess();
        }
        System.out.println("Begin procedure ended for: " + transfer.getSourceDeviceId() + " -(" + transfer.getComponentId() + ")-" + transfer.getDestinationDeviceId());
        transferBeginProcedureLock.release();

//        System.out.println("Begin procedure ended for: " + sourceDevice.getId() + " -(" + transferredComponent.getId() + ")-" + destinationDevice.getId());

        transferredComponent.getWakeCallToPrepare().acquire();

        if (sourceDevice != null) {
            sourceDevice.acquireAccess();

            //  Picking up transfer that can be prepared simultaneously.
            if (transferredComponent.getNextToTransfer() == null)
                transferredComponent.setNextToTransfer(
                        sourceDevice.getPrepareWaitLineIn().poll());

            // Waking up transfer that reserved space after component moved in this transfer.
            if (transferredComponent.getNextToTransfer() != null)
                transferredComponent.getNextToTransfer().getWakeCallToPrepare().release();

            // If we didn't pick up any transfer, then leaving sign that one incoming can be prepared.
            if (transferredComponent.getNextToTransfer() == null)
                sourceDevice.getExecuteWaitSetOut().add(transferredComponent);

            sourceDevice.releaseAccess();
        }

        transfer.prepare();

        if (sourceDevice != null) {
            sourceDevice.acquireAccess();

            // Removing transferred component from sourceDevice component count.
            sourceDevice.chgStillCompCnt(-1);
            // Removing option for left transfers to reserve space after transferred component.
            sourceDevice.getExecuteWaitSetOut().remove(transferredComponent);
            // Waking up transfer that reserved space after component moved in this transfer.
            if (transferredComponent.getNextToTransfer() != null)
                transferredComponent.getNextToTransfer().getWakeCallToExecute().release();

            sourceDevice.releaseAccess();
        }

        transferredComponent.getWakeCallToExecute().acquire();


        // No need to change anything on destinationDevice,
        // since component is moving in space left after some other component.

        transfer.perform();

        if (destinationDevice == null)
            components.remove(transferredComponentId, transferredComponent);
        transferredComponent.finishTransfer();

    } catch (InterruptedException e) {
        // From task assumption we don't need to worry about InterruptedException.
        throw new RuntimeException("panic: unexpected thread interruption");
    }
    }
    private class Component {
        private final ComponentId id;
        private volatile Device currentDevice;
        private Semaphore wakeCallToPrepare = new Semaphore(0, true);
        private Semaphore wakeCallToExecute = new Semaphore(0, true);
        private volatile Component nextToExecute = null;
        private volatile ComponentTransfer currentTransfer = null;
        private Semaphore lock = new Semaphore(1, true);
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

        public Semaphore getWakeCallToExecute() {
            return wakeCallToExecute;
        }

        public void setNextToTransfer(Component nextToExecute) {
            this.nextToExecute = nextToExecute;
        }

        public Component getNextToTransfer() {
            return nextToExecute;
        }

        public Semaphore getLock() {
            return lock;
        }
//        private final Semaphore accessTransfer = new Semaphore(1, true);
        public void setTransfer(ComponentTransfer t) {
                currentTransfer = t;
//            try {
//                accessTransfer.acquire();
//            } catch (InterruptedException e) {
//                throw new RuntimeException("panic: unexpected thread interruption");
//            }
//            if (currentTransfer == null) {
//                currentTransfer = t;
//                accessTransfer.release();
//                return true;
//            }
//            accessTransfer.release();
//            return false;
        }

        public ComponentTransfer getCurrentTransfer() {
            return currentTransfer;
        }
        public void finishTransfer() {
            nextToExecute = null;
            wakeCallToExecute.release();
            wakeCallToPrepare = new Semaphore(0, true);
            wakeCallToExecute = new Semaphore(0, true);
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
    private class Device {
        private final DeviceId id;
        private final int totalSpots;
        private Semaphore access = new Semaphore(1, true);
//        private BlockingQueue<Component> stillComponentList;
        private volatile int stillCompCnt;
        private BlockingQueue<Component> prepareWaitLineIn = new LinkedBlockingQueue<Component>();
//        private BlockingQueue<Component> prepareWaitLineOut = new LinkedBlockingQueue<Component>();
//        private BlockingQueue<Component> executeWaitLineIn = new LinkedBlockingQueue<Component>();
//        private BlockingQueue<Component> executeWaitLineOut = new LinkedBlockingQueue<Component>();
        private Set<Component> executeWaitSetOut = new HashSet<>();
        public Device(DeviceId id, int totalSpots, Queue<Component> storage) {
            this.id = id;
            this.totalSpots = totalSpots;
//            this.stillComponentList = new LinkedBlockingQueue(storage);
            this.stillCompCnt = storage.size();
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

//        public Queue<Component> getStillComponentList() {
//            return stillComponentList;
//        }

        public int getStillCompCnt() {
            return stillCompCnt;
        }
        public void chgStillCompCnt(int inc) {
            stillCompCnt += inc;
        }

        public Queue<Component> getPrepareWaitLineIn() {
            return prepareWaitLineIn;
        }
//        public Queue<Component> getPrepareWaitLineOut() {
//            return prepareWaitLineOut;
//        }
//        public Queue<Component> getExecuteWaitLineIn() {
//            return executeWaitLineIn;
//        }
//        public Queue<Component> getExecuteWaitLineOut() {
//            return executeWaitLineOut;
//        }

        public Set<Component> getExecuteWaitSetOut() {
            return executeWaitSetOut;
        }
        public static Boolean findCycle(Device v, Set<DeviceId> seen, Stack<Component> currentPath, DeviceId hasLockOn) {
            if (!v.getId().equals(hasLockOn))
                v.acquireAccess();
            try {

                seen.add(v.getId());
                Iterator<Component> iter = v.getPrepareWaitLineIn().iterator();

                while (iter.hasNext()) {
                    Component x = iter.next();
                    Device xd = x.getCurrentDevice();

                    if (xd == null)
                        continue;

                    currentPath.push(x);
                    if (seen.contains(xd.getId())) {
                        iter.remove();
                        return true;
                    }

                    if (findCycle(xd, seen, currentPath, hasLockOn)) {
                        iter.remove();
                        return true;
                    }

                    currentPath.pop();
                }
                seen.remove(v.getId());

                return false;
            } finally {
                if (!v.getId().equals(hasLockOn))
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
