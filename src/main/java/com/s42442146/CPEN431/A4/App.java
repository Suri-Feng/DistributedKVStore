package com.s42442146.CPEN431.A4;

import com.s42442146.CPEN431.A4.model.KVServer;


public class App 
{
    public static void main( String[] args )
    {

            new KVServer(Integer.parseInt(args[0])).start();

    }
}
