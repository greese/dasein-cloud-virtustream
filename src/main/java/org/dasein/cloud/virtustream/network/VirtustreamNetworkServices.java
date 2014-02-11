package org.dasein.cloud.virtustream.network;

import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.virtustream.Virtustream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VirtustreamNetworkServices extends AbstractNetworkServices {
    private Virtustream cloud = null;

    public VirtustreamNetworkServices(@Nonnull Virtustream cloud) {
        this.cloud = cloud;
    }

    @Nullable
    @Override
    public Networks getVlanSupport() {
        return new Networks(cloud);
    }
}
