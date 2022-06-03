package dynamok.transformers;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.nsl.logical.model.TransactionDto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

public final class CompressionUtils {
    private CompressionUtils() {
    }

    public static String compressAndReturnB64(String text) throws IOException {
        return new String(Base64.getEncoder().encode(compress(text)));
    }

    public static byte[] compress(String text) throws IOException {
        return compress(text.getBytes());
    }

    public static ByteBuffer compressString(String inputJson) throws IOException {
        String input = compressAndReturnB64(new Gson().fromJson(inputJson, JsonObject.class).toString());
        // Compress the UTF-8 encoded String into a byte[]
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream os = new GZIPOutputStream(baos);
        os.write(input.getBytes(StandardCharsets.UTF_8));
        os.close();
        baos.close();
        byte[] compressedBytes = baos.toByteArray();

        ByteBuffer buffer = ByteBuffer.allocate(compressedBytes.length);
        buffer.put(compressedBytes, 0, compressedBytes.length);
        buffer.position(0); // Important: reset the position of the ByteBuffer
        // to the beginning
        return buffer;
    }

    public static String getTransactionUniqueId(String tenantId, String txnId, String gsiContextualId) {
        if (gsiContextualId == null || gsiContextualId.isBlank() || "null".equals(gsiContextualId)) {
            return new StringBuilder(tenantId).append("_").append(txnId).toString();
        } else {
            return new StringBuilder(tenantId).append("_").append(txnId).append("_").append(gsiContextualId).toString();
        }
    }

    private static byte[] compress(byte[] bArray) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (DeflaterOutputStream dos = new DeflaterOutputStream(os)) {
            dos.write(bArray);
        }
        return os.toByteArray();
    }


}
