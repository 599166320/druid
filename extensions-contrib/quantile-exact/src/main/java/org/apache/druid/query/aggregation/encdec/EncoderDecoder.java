package org.apache.druid.query.aggregation.encdec;

import net.jpountz.lz4.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class EncoderDecoder {

    public static byte[] compressedByte(byte[] data) {
        final int decompressedLength = data.length;
        LZ4Factory factory = LZ4Factory.fastestInstance();
        // compress data
        LZ4Compressor compressor = factory.fastCompressor();
        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(data, 0, decompressedLength, compressed, 0, maxCompressedLength);
        byte[] dest = new byte[compressedLength];
        System.arraycopy(compressed,0,dest,0,compressedLength);
        return dest;
    }

    public static byte[] decompressorByte(byte[] compressorByte, int srcLength) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompressor = factory.fastDecompressor();
        byte[] restored = new byte[srcLength];
        decompressor.decompress(compressorByte, 0, restored, 0, srcLength);
        return restored;
    }

    public static byte[] lz4Compress(byte[] srcByte, int blockSize) throws IOException, IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        LZ4Compressor compressor = factory.fastCompressor();
        LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(byteOutput, blockSize, compressor);
        compressedOutput.write(srcByte);
        compressedOutput.close();
        return byteOutput.toByteArray();
    }

    public static byte[] lz4Decompress(byte[] compressorByte, int blockSize) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(blockSize);
        LZ4FastDecompressor decompresser = factory.fastDecompressor();
        LZ4BlockInputStream lzis = new LZ4BlockInputStream(new ByteArrayInputStream(compressorByte), decompresser);
        int count;
        byte[] buffer = new byte[blockSize];
        while ((count = lzis.read(buffer)) != -1) {
            baos.write(buffer, 0, count);
        }
        lzis.close();
        return baos.toByteArray();
    }


    public static byte[] returnFileByte(String filePath) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(new File(filePath));
        FileChannel channel = fileInputStream.getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) channel.size());
        channel.read(byteBuffer);
        return byteBuffer.array();
    }


    public static void createFile(byte[] fileByte, String filePath) {
        BufferedOutputStream bufferedOutputStream;
        FileOutputStream fileOutputStream;
        File file = new File(filePath);
        try {
            fileOutputStream = new FileOutputStream(file);
            bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
            bufferedOutputStream.write(fileByte);
            fileOutputStream.close();
            bufferedOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
