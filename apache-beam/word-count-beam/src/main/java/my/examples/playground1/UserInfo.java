package my.examples.playground1;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
class UserInfo {
    private String name;
    private String email;

    UserInfo() {}

    UserInfo(String name, String email) {
        this.name = name;
        this.email = email;
    }

    UserInfo encryptEmail() {
        return new UserInfo(this.name, Encrypter.encrypt(this.email));
    }

    String toCSV() {
        return name + "," + email;
    }
}
