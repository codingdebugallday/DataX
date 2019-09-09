package org.abigballofmud.datax.plugin.reader.otspostprocessreader.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.alibaba.datax.common.exception.DataXException;
import org.abigballofmud.datax.plugin.reader.otspostprocessreader.OtsPostProcessReaderError;

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/09/09 10:23
 * @since 1.0
 */
public class ZlibUtil {

    private ZlibUtil() {
        throw new IllegalStateException("util class");
    }

    public static byte[] compress(byte[] data) {
        Deflater deflater = new Deflater();
        deflater.reset();
        deflater.setInput(data);
        deflater.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
        try {
            byte[] buf = new byte[1024];
            while (!deflater.finished()) {
                int i = deflater.deflate(buf);
                bos.write(buf, 0, i);
                bos.flush();
            }
            byte[] output = bos.toByteArray();
            deflater.end();
            return output;
        } catch (IOException e) {
            throw DataXException.asDataXException(OtsPostProcessReaderError.ERROR, Common.getDetailMessage(e), e);
        } finally {
            try {
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static byte[] decompress(byte[] data) {
        Inflater inflater = new Inflater();
        inflater.reset();
        inflater.setInput(data);
        ByteArrayOutputStream o = new ByteArrayOutputStream(data.length);
        try {
            byte[] buf = new byte[1024];
            while (!inflater.finished()) {
                int i = inflater.inflate(buf);
                o.write(buf, 0, i);
                o.flush();
            }
            byte[] output = o.toByteArray();
            inflater.end();
            return output;
        } catch (DataFormatException e) {
            throw DataXException.asDataXException(OtsPostProcessReaderError.ERROR, Common.getDetailMessage(e), e);
        } catch (IOException e) {
            throw DataXException.asDataXException(OtsPostProcessReaderError.ERROR, Common.getDetailMessage(e), e);
        } finally {
            try {
                o.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
