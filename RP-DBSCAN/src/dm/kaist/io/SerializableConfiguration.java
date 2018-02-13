package dm.kaist.io;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;

public class SerializableConfiguration extends Configuration implements Serializable
{

	public SerializableConfiguration() {
		super();
		// TODO Auto-generated constructor stub
	}

	public SerializableConfiguration(Configuration other) {
		super(other);
		// TODO Auto-generated constructor stub
	}
	
}
