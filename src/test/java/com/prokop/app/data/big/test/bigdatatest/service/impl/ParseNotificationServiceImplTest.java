package com.prokop.app.data.big.test.bigdatatest.service.impl;

import com.prokop.app.data.big.test.bigdatatest.model.FileIdentifier;
import com.prokop.app.data.big.test.bigdatatest.service.ParseNotificationService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class ParseNotificationServiceImplTest {
    private final String data1 = "{\n" +
            "  \"kind\": \"storage#object\",\n" +
            "  \"id\": \"big-data-test-app-bucket/test-folder//1605870315052428\",\n" +
            "  \"selfLink\": \"https://www.googleapis.com/storage/v1/b/big-data-test-app-bucket/o/test-folder%2F\",\n" +
            "  \"name\": \"avro.sample\",\n" +
            "  \"bucket\": \"big-data-test-app-bucket\",\n" +
            "  \"generation\": \"1605870315052428\",\n" +
            "  \"metageneration\": \"1\",\n" +
            "  \"contentType\": \"text/plain\",\n" +
            "  \"timeCreated\": \"2020-11-20T11:05:15.052Z\",\n" +
            "  \"updated\": \"2020-11-20T11:05:15.052Z\",\n" +
            "  \"storageClass\": \"STANDARD\",\n" +
            "  \"timeStorageClassUpdated\": \"2020-11-20T11:05:15.052Z\",\n" +
            "  \"size\": \"0\",\n" +
            "  \"md5Hash\": \"1B2M2Y8AsgTpgAmY7PhCfg==\",\n" +
            "  \"mediaLink\": \"https://www.googleapis.com/download/storage/v1/b/big-data-test-app-bucket/o/test-folder%2F?generation=1605870315052428&alt=media\",\n" +
            "  \"crc32c\": \"AAAAAA==\",\n" +
            "  \"etag\": \"CIy7vI39kO0CEAE=\",\n" +
            "  \"temporaryHold\": false,\n" +
            "  \"eventBasedHold\": false\n" +
            "}";

    private final String data2 = "{\n" +
            "  \"kind\": \"storage#object\",\n" +
            "  \"id\": \"big-data-test-app-bucket/test-folder//1605870315052428\",\n" +
            "  \"selfLink\": \"https://www.googleapis.com/storage/v1/b/big-data-test-app-bucket/o/test-folder%2F\",\n" +
            "  \"name\": \"another.avro.sample\",\n" +
            "  \"bucket\": \"big-data-test-app-bucket\",\n" +
            "  \"generation\": \"1605870315052428000000\",\n" +
            "  \"metageneration\": \"1\",\n" +
            "  \"contentType\": \"text/plain\",\n" +
            "  \"timeCreated\": \"2020-11-20T11:05:15.052Z\",\n" +
            "  \"updated\": \"2020-11-20T11:05:15.052Z\",\n" +
            "  \"storageClass\": \"STANDARD\",\n" +
            "  \"timeStorageClassUpdated\": \"2020-11-20T11:05:15.052Z\",\n" +
            "  \"size\": \"0\",\n" +
            "  \"md5Hash\": \"1B2M2Y8AsgTpgAmY7PhCfg==\",\n" +
            "  \"mediaLink\": \"https://www.googleapis.com/download/storage/v1/b/big-data-test-app-bucket/o/test-folder%2F?generation=1605870315052428&alt=media\",\n" +
            "  \"crc32c\": \"AAAAAA==\",\n" +
            "  \"etag\": \"CIy7vI39kO0CEAE=\",\n" +
            "  \"temporaryHold\": false,\n" +
            "  \"eventBasedHold\": false\n" +
            "}";

    private FileIdentifier expectedResult;
    private FileIdentifier actualResult;
    private ParseNotificationService parseNotificationService = new ParseNotificationServiceImpl();

    @Test
    public void testGetFileIdentifier_ShouldReturnValidFileIdentifierObject() {
        expectedResult = new FileIdentifier("avro.sample", "1605870315052428");
        actualResult = parseNotificationService.getFileIdentifier(data1);
        assertEquals(expectedResult, actualResult);

        expectedResult = new FileIdentifier("another.avro.sample", "1605870315052428000000");
        actualResult = parseNotificationService.getFileIdentifier(data2);
        assertEquals(expectedResult, actualResult);
    }

    @Test(expected = java.lang.AssertionError.class)
    public void testGetFileIdentifier_ShouldReturnAssertionErrorForIntentionallyWrongInput() {
        expectedResult = new FileIdentifier("Something wrong", "Something wrong");
        actualResult = parseNotificationService.getFileIdentifier(data1);
        assertEquals(expectedResult, actualResult);
    }

    @Test(expected = java.lang.NullPointerException.class)
    public void testGetFileIdentifier_ShouldReturnNullPointerExceptionForNullInput() {
        expectedResult = new FileIdentifier("avro.sample", "1605870315052428");
        actualResult = parseNotificationService.getFileIdentifier(null);
        assertEquals(expectedResult, actualResult);
    }
}
