package org.apache.directmemory.conf;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class YamlConfiguration extends DefaultConfiguration
{

    public static ConfigurationService load()
    {
        Yaml yaml = new Yaml( new Constructor( YamlConfiguration.class ) );
        return (YamlConfiguration) yaml.load( Configuration.class.getClassLoader().getResourceAsStream( "directmemory.yaml" ) );
    }

}
