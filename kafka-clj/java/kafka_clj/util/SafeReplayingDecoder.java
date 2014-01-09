package kafka_clj.util;

import io.netty.handler.codec.ReplayingDecoder;

/**
 * 
 * Makes the checkpoint and state methods public, so that no reflection is needed from clojure.
 * 
 */
public abstract class SafeReplayingDecoder extends ReplayingDecoder<Object>{

	public SafeReplayingDecoder(Object s){
		super(s);
	}
	
	public final void checkp(Object obj){
		checkpoint(obj);
	}

	public final Object getState(){
		return state();
	}
	
}
