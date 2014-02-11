package org.dasein.cloud.virtustream.compute;

import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.virtustream.Virtustream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VirtustreamComputeServices extends AbstractComputeServices {
    private Virtustream cloud = null;

    public VirtustreamComputeServices(@Nonnull Virtustream cloud) {
        this.cloud = cloud;
    }

    @Nullable
    @Override
    public Templates getImageSupport() {
        return new Templates(cloud);
    }

    @Nullable
    @Override
    public VirtualMachines getVirtualMachineSupport() {
        return new VirtualMachines(cloud);
    }

    @Nullable
    @Override
    public Volumes getVolumeSupport() {
        return new Volumes(cloud);
    }
}
