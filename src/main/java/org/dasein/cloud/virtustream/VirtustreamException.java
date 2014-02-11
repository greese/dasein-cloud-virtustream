package org.dasein.cloud.virtustream;

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

public class VirtustreamException extends CloudException {
    public VirtustreamException(@Nonnull Throwable cause) {
        super(cause);
    }

    public VirtustreamException(@Nonnull CloudErrorType type, @Nonnegative int httpCode, @Nonnull String providerCode, @Nonnull String message) {
        super(type, httpCode, providerCode, message);
    }
}