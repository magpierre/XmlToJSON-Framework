/*
 * Copyright MapR Technologies, $year
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package  streamflow.bolt.XmlToJson;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.json.simple.JSONObject;
import org.xml.sax.SAXException;
import streamflow.xml.parser.MySaxParser;

import com.google.inject.Inject;
import com.google.inject.name.Named;

import streamflow.annotations.Component;
import streamflow.annotations.ComponentOutputs;
import streamflow.annotations.ComponentProperty;
import streamflow.annotations.Description;
import streamflow.annotations.ComponentInterface;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by mpierre on 15-10-14.
 */
@Component(label = "XML TO JSON bolt", name = "XMLTOJSON-bolt", type = "storm-bolt", icon="")
@Description("Parses and generates JSON documents")
@ComponentOutputs({@ComponentInterface(key = "default", description = "any")})
public class ParseXMLTOJSONBolt extends BaseRichBolt {

    SAXParserFactory parserFactor = null;
    SAXParser parser = null;
    MySaxParser handler;
    private  OutputCollector _collector;

    public boolean getRemoveNameSpace() {
        return removeNameSpace;
    }

    @ComponentProperty(label = "Remove Namespace From Tags", name = "removeNameSpace", required = true, type = "boolean", defaultValue ="true")
    @Description("Controls if namespace should be part of tags")
    @Inject
    public void setRemoveNameSpace(@Named("removeNameSpace") boolean removeNameSpace) {
        this.removeNameSpace = removeNameSpace;
    }

    boolean removeNameSpace = false;


    private static final long serialVersionUID = -325235325236L;
    private Logger LOG;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        try {
            parserFactor = SAXParserFactory.newInstance();
            parser = parserFactor.newSAXParser();
            handler = new MySaxParser();
            handler.setRemoveNameSpace(getRemoveNameSpace());
            handler.setLogger(LOG);

            _collector = collector;

        } catch (SAXException e) {

            LOG.error(e.getMessage());

        }
        catch (ParserConfigurationException e) {

            LOG.error(e.getMessage());

        }
    }



    @Override
    public void execute(Tuple input) {

        try {
            File p = (File) input.getValue(0);


            LOG.info("Executing on: " + p.getAbsolutePath());
            parser.parse(p, handler);

            JSONObject o = handler.getVal();

            LOG.info("Output of file: " + p.getAbsoluteFile());



            _collector.emit(input, new Values(p, o));

            _collector.ack(input);

        } catch (IOException e) {
            LOG.error(e.getMessage());

        }
        catch (SAXException e) {
            LOG.error(e.getMessage());

        }

    }

    @Inject
    public void setLogger(Logger logger){
        this.LOG = logger;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
       declarer.declare(true,new Fields("file","generatedDoc"));

    }
}
