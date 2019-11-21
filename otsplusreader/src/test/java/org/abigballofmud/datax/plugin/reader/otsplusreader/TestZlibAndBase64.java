package org.abigballofmud.datax.plugin.reader.otsplusreader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.InflaterOutputStream;

import org.abigballofmud.datax.plugin.reader.otsplusreader.utils.ZlibUtil;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;
import sun.misc.BASE64Decoder;

/**
 * <p>
 * description
 * </p>
 *
 * @author isacc 2019/09/09 15:31
 * @since 1.0
 */
public class TestZlibAndBase64 {

    @Test
    public void testZlibAndBase64() throws IOException {
        String str = "86439769|25.70442681206597|117.677353515625|0598";
        String encodeStr = "eJwFwYENADAIArCPFkUB/c3j14661toDn6MbmkSI68v0k10sJgVecOcDBIgKrw==";
        // 加密 先zlib再base64
        byte[] compress = ZlibUtil.compress(str.getBytes());
        String encode = new String(Base64.encodeBase64(compress));
        System.out.println("加密后:" + encode);
        // 解密 先base64再zlib
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        InflaterOutputStream zos = new InflaterOutputStream(bos);
        zos.write(Base64.decodeBase64(encodeStr.getBytes()));
        zos.flush();
        zos.close();
        String decodeStr = new String(bos.toByteArray());
        System.out.println("解密后:" + decodeStr);
        byte[] decodeBuffer = new BASE64Decoder().decodeBuffer(encode);
        String decode = new String(ZlibUtil.decompress(decodeBuffer));
        System.out.println("解密后:" + decode);
        Assert.assertEquals(str, decode);
    }

}
