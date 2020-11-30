package com.prokop.app.data.big.test.bigdatatest.repository.impl;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.prokop.app.data.big.test.bigdatatest.exception.LoadFileToBigQueryException;
import com.prokop.app.data.big.test.bigdatatest.repository.BigQueryRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest()
@RunWith(SpringRunner.class)
public class BigQueryRepositoryImplTest {
    @Mock
    BigQuery mockBigQuery;

    @Mock
    BigQueryRepository mockBigQueryRepository;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testLoadBytesToBigQuery() throws LoadFileToBigQueryException {
        mockBigQuery = BigQueryOptions.getDefaultInstance().getService();

        doNothing().when(mockBigQueryRepository).loadBytesToBigQuery(isA(byte[].class), isA(String.class),
                isA(String.class), isA(BigQuery.class));
        mockBigQueryRepository.loadBytesToBigQuery(new byte[] {10, 20, 30, 40}, "test-dataset",
                "test-table-name", mockBigQuery);

        verify(mockBigQueryRepository, times(1)).loadBytesToBigQuery(
                new byte[] {10, 20, 30, 40}, "test-dataset", "test-table-name", mockBigQuery);
    }

    @Test(expected = LoadFileToBigQueryException.class)
    public void testLoadBytesToBigQuery_ThrowLoadFileToBigQueryException() throws LoadFileToBigQueryException {
        doThrow(new LoadFileToBigQueryException("Error while loading file to BigQuery."))
                .when(mockBigQueryRepository).loadBytesToBigQuery(new byte[] {10, 20, 30, 40},
                "test-dataset", "test-table-name", mockBigQuery);
        mockBigQueryRepository.loadBytesToBigQuery(new byte[] {10, 20, 30, 40},
                "test-dataset", "test-table-name", mockBigQuery);
    }
}
