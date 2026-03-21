package com.salesforce.mcg.datasync.batch;

import org.springframework.batch.item.file.BufferedReaderFactory;
import org.springframework.core.io.Resource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;

/**
 * Creates a BufferedReader that transparently handles .gz (by magic bytes) or plain text.
 */
public class CompressBufferedReaderFactory implements BufferedReaderFactory {

    @Override
    public BufferedReader create(Resource resource, String encoding) throws IOException {
        InputStream in = resource.getInputStream();
        PushbackInputStream pb = new PushbackInputStream(in, 2);
        byte[] sig = new byte[2];
        int n = pb.read(sig);
        if (n > 0) pb.unread(sig, 0, n);

        boolean isGzip = n >= 2 && (sig[0] == (byte)0x1F && sig[1] == (byte)0x8B);

        InputStream data = pb;
        if (isGzip) {
            data = new GZIPInputStream(pb);
        }
        // Removed ZIP handling - now only supports GZIP and plain text files
        return new BufferedReader(new InputStreamReader(data, StandardCharsets.UTF_8));
    }
}
