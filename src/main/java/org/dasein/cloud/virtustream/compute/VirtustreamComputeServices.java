/**
 * Copyright (C) 2012-2013 Dell, Inc.
 * See annotations for authorship information
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

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
