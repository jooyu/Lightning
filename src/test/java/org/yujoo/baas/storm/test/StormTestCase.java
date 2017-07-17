package org.yujoo.baas.storm.test;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;

import backtype.storm.tuple.Tuple;

/**
 * Created with IntelliJ IDEA.
 * User: admin
 * Date: 2012/12/09
 * Time: 8:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class StormTestCase {
    
	protected Mockery context = new Mockery() {{
        setImposteriser(ClassImposteriser.INSTANCE);
    }};
    
    

    protected Tuple getTuple(){
        final Tuple tuple = context.mock(Tuple.class);
        return tuple;
    }


}
