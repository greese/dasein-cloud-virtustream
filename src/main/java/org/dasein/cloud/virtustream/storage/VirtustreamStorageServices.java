package org.dasein.cloud.virtustream.storage;

import org.dasein.cloud.storage.AbstractStorageServices;
import org.dasein.cloud.virtustream.Virtustream;

import javax.annotation.Nullable;

public class VirtustreamStorageServices extends AbstractStorageServices{
    private Virtustream provider;

    public VirtustreamStorageServices(Virtustream provider) { this.provider = provider; }

    @Nullable
    @Override
    public BlobStore getOnlineStorageSupport() {
        return new BlobStore(provider);
    }
}
