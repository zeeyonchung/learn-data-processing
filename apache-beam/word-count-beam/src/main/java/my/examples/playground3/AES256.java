package my.examples.playground3;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;

public class AES256 {
    private static final String ALGORITHMS = "AES/CBC/PKCS5Padding";
    private static final String ALGORITHM = "AES";
    private static final String CHARSET = "UTF-8";

    public static String encrypt(String encKey, String plainText) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGORITHMS);
        SecretKeySpec keySpec = new SecretKeySpec(encKey.getBytes(), ALGORITHM);
        String iv = encKey.substring(0, 16);
        IvParameterSpec ivParamSpec = new IvParameterSpec(iv.getBytes());
        cipher.init(Cipher.ENCRYPT_MODE, keySpec, ivParamSpec);

        byte[] encrypted = cipher.doFinal(plainText.getBytes());
        return Base64.getEncoder().encodeToString(encrypted);
    }

    public static String decrypt(String decKey, String cipherText) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGORITHMS);
        SecretKeySpec keySpec = new SecretKeySpec(decKey.getBytes(), ALGORITHM);
        String iv = decKey.substring(0, 16);
        IvParameterSpec ivParamSpec = new IvParameterSpec(iv.getBytes());
        cipher.init(Cipher.DECRYPT_MODE, keySpec, ivParamSpec);

        byte[] decodedBytes = Base64.getDecoder().decode(cipherText);
        byte[] decrypted = cipher.doFinal(decodedBytes);
        return new String(decrypted, CHARSET);
    }
}
