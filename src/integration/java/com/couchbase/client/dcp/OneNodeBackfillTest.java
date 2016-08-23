package com.couchbase.client.dcp;

import com.couchbase.client.dcp.util.CouchbaseContainer;
import org.junit.ClassRule;
import org.junit.Test;

public class OneNodeBackfillTest {

    @ClassRule
    public static CouchbaseContainer db = new CouchbaseContainer();

    @Test
    public void foo() throws Exception {
        Thread.sleep(1000000);
    }

}
