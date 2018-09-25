package com.ais.common.tools;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by ShiGZ on 2017/5/18.
 */
public class SplitKeyTool {

    public static  byte[][] calcSplitKeys() {
        byte[][] splitKeys = new byte[9][];
        for(int i = 1; i < 10 ; i ++) {
            splitKeys[i-1] = Bytes.toBytes("" + i);
        }
        return splitKeys;
    }

}
