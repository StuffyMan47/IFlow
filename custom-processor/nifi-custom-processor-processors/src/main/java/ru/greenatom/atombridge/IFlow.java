/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.greenatom.atombridge;

import groovy.util.XmlParser;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;

import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

//Импорты из груви скрипта
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.serialization.record.Record;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import javax.xml.xpath.*;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;

import org.apache.nifi.provenance.ProvenanceReporter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.nifi.components.PropertyValue;
import java.util.Arrays;
import org.apache.nifi.lookup.StringLookupService;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.*;

import org.codehaus.groovy.ant.Groovy;
import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.json.XML;
import org.w3c.dom.Element;
import org.json.JSONArray;
import org.json.JSONObject;
import org.xml.sax.SAXException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.charset.Charset;

import org.apache.nifi.stream.io.StreamUtils;

import groovy.lang.Binding;
import java.util.logging.Logger;

import static org.codehaus.groovy.tools.xml.DomToGroovy.parse;

import org.w3c.dom.*;
import javax.xml.xpath.*;
import javax.xml.parsers.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class IFlow extends AbstractProcessor {

    private Object xslRemoveEnv = null;
    private String traceOut;
    private int traceCount = 0;
    private String traceOut1;
    private int traceCount1 = 0;

    private final ComponentLog logger = getLogger();

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("My property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Distributed Cache Service")
            .description("The Controller Service that is used to get the cached values.")
            .required(true)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    public static final Relationship Load = new Relationship.Builder()
            .name("Load")
            .description("Example relationship")
            .build();

    public static final Relationship Transform = new Relationship.Builder()
            .name("Transform")
            .description("Example relationship")
            .build();

    public static final Relationship Failure = new Relationship.Builder()
            .name("Failure")
            .description("Example relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MY_PROPERTY);
        descriptors.add(PROP_DISTRIBUTED_CACHE_SERVICE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(Load);
        relationships.add(Transform);
        relationships.add(Failure);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }



    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }



        XPath xpath = XPathFactory.newInstance().newXPath();
        try {
            var builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            throw new RuntimeException(e);
        }

        /** Посмотреть откуда берётся IflowMapCacheLookupClient и т.д **/
        //todo Пофиксиить
        String iflowMapCacheLookupClientName = IflowMapCacheLookupClient.getValue();
        String xsdMapCacheLookupClientName = XsdMapCacheLookupClient.getValue();
        String xsltMapCacheLookupClientName = XsltMapCacheLookupClient.getValue();


        try {
            var iflowCacheMap = getServiceController(iflowMapCacheLookupClientName, context);
            var xsdCacheMap = getServiceController(xsdMapCacheLookupClientName, context);
            var xsltCacheMap = getServiceController(xsltMapCacheLookupClientName, context);

            String ret = iflowCacheMap.get(flowFile.getAttribute("business.process.name"),
                    new Serializer<String>(){

                        @Override
                        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
                            out.write(value.getBytes(StandardCharsets.UTF_8));
                        }

                    },
                    new Deserializer<String>(){

                        @Override
                        public String deserialize(final byte[] value) throws DeserializationException, IOException {
                            if (value == null) {
                                return null;
                            }
                            return new String(value, StandardCharsets.UTF_8);

                        }});
            if (ret != null) {
                trace("iFlow not found, return 501");
                logger.error( "iFlow named:" + flowFile.getAttribute("business.process.name") + "not found!");
                session.putAttribute(flowFile, "iflow.error", "iFlow named:" + flowFile.getAttribute("business.process.name") + "not found!")
                session.putAttribute(flowFile, "iflow.status.code", getResponse("", "501"));
                session.transfer(flowFile, Failure);
                return;
            } else {
                trace("readed iFlow config");
            }
            trace("start parsing json iFlow");

            //todo ret всегда null перепроверить
//            var iflow = new groovy.json.JsonSlurper().parseText(ret);
            JSONObject iflow = new JSONObject(ret);
            JSONArray targets = iflow.getJSONArray("targets");
            trace("full list of defined target systems" + targets);

            boolean sync = Boolean.parseBoolean(iflow.getString("sync"));
            int numOfTargets = targets.length();

            if (flowFile.getAttribute("xform.stage") != null & flowFile.getAttribute("target.id") != null & flowFile.getAttribute("xform.path") != null) {
                try {
                    trace1("+loopback " + flowFile.getAttribute("xform.stage"));
                    String targetId = flowFile.getAttribute("target.id");
                    int targetIndx = findTarget(targets, targetId);
                    if (targetIndx < 0) throw new IllegalArgumentException('Target not found')
                    def target = targets.get(targetIndx);

                    ArrayList xforms = target.transformations as ArrayList

                    int xformPath = Integer.parseInt(flowFile.getAttribute('xform.path'));

                    if (xformPath > -1 & xformPath < xforms.size()) {
                        if (target.output ==" 'JSON'") flowFile.putAttribute("target.output", "JSON");
                        def xform = xforms.get(xformPath);
                        def result = processXform(flowFile, xform, targetId);
                        if (result == null) {
                            trace("-ff");
                            session.remove(flowFile);
                        } else {
                            List urlList = target.targetPath instanceof List ? target.targetPath : [target.targetPath];
                            transferResult(result, sync, urlList, target);
                        }
                    } else {
                        throw new Exception("Incorrect transformation path " + xformPath);
                    }
                } catch (Exception ex1) {
                    trace("!!!!!!!Exception: ${ex1.toString()}");
                    StringBuilder exMsgBldr = new StringBuilder();
                    exMsgBldr.append("Exception '${ex1.toString()}' occurs");
                    exMsgBldr.append(" while processing FlowFile '${flowFile.getAttribute('filename')}'");

                    exMsgBldr.append(" in '${flowFile.getAttribute('business.process.name')}' scenario");
                    exMsgBldr.append(" at '${flowFile.getAttribute('target.id')}' target");
                    exMsgBldr.append(" at '${flowFile.getAttribute('xform.path')}' path");
                    exMsgBldr.append(" at ${flowFile.getAttribute('xform.stage')} stage");

                    session.putAttribute(flowFile, "error.msg", ex1.toString());

                    log.log(org.apache.nifi.logging.LogLevel.ERROR, exMsgBldr.toString())
                    log.log(org.apache.nifi.logging.LogLevel.ERROR, traceOut);

                    session.putAttribute(flowFile, 'error.msg', ex1.toString())

                    //failFlowFiles << flowFileCopy
                    session.transfer(flowFile, REL_FAILURE)
                    return
                }
            } else {
                //Validate against xsd schema
                flowFile."xform.stage" == -1;
                String schemaContent = null;
                boolean isFailedSchemaExtraction = false;
                try {
                    schemaContent = xsdCacheMap.get(iflow.validate,
                            new Serializer<String>(){

                                @Override
                                public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
                                    out.write(value.getBytes(StandardCharsets.UTF_8))
                                }

                            },
                            new Deserializer<String>(){

                                @Override
                                public String deserialize(final byte[] value) throws DeserializationException, IOException {
                                    if (value == null) {
                                        return null
                                    }
                                    return new String(value, StandardCharsets.UTF_8)

                                }});
                    if (!schemaContent) {
                        throw new IOException("Schema with name ${iflow.validate} not found")
                    }
                } catch (Exception e) {
                    String msg = 'Failed schema extraction! ' + e;
                    log.error msg;
                    flowFile.'error.msg' = msg;
                    session.transfer(flowFile, REL_FAILURE);
                    isFailedSchemaExtraction = true;
                }
                if (isFailedSchemaExtraction) return

                        InputStream fis = null;
                boolean isFailedValidation = false;
                try {
                    fis = session.read(flowFile)
                    Source xmlFile = new StreamSource(fis);
                    javax.xml.validation.SchemaFactory schemaFactory = SchemaFactory
                            .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                    javax.xml.validation.Schema schema = schemaFactory.newSchema(new StreamSource(new StringReader(schemaContent)))
                    javax.xml.validation.Validator validator = schema.newValidator();
                    validator.validate(xmlFile);
                } catch (Exception e) {
                    String msg = 'Failed xml validation! ' + e;
                    log.error msg;
                    flowFile.'error.msg' = msg;
                    isFailedValidation = true;
                    session.transfer(flowFile, Failure);
                } finally {
                    fis.close();
                }
                if (isFailedValidation) return

                        ProvenanceReporter reporter = session.getProvenanceReporter();

                targets.eachWithIndex { it, flowIndex ->
                        ArrayList xforms = it.transformations as ArrayList
                    //Make a copy of incoming flow file for each target system
                    //Or use the incoming flowfile for last target
                    //FlowFile file = flowIndex < numOfTargets - 1 & numOfTargets > 1 ? session.clone(flowFile) : flowFile
                    FlowFile file = null
                    if(flowIndex < numOfTargets - 1 & numOfTargets > 1){
                        file = session.clone(flowFile);
                        reporter.clone(flowFile, file);
                    } else{
                        file = flowFile;
                    }
                    session.putAttribute(file, "Receiver", it.id);
                    int xformPath = -1;

                    if (it.syncValidation == "true") {
                        syncResponse(file);
                    }

                    if (it.output == 'JSON') {
                        session.putAttribute(file, "target.output", "JSON")
                    }

                    FlowFile f = null;

                    for (ArrayList xform : xforms) {
                        try {
                            xformPath++
                            session.putAttribute(file, "xform.path", String.valueOf(xformPath));
                            f = xformPath < xforms.size() - 1 & xforms.size() > 1 ? session.clone(file) : file;

                            def result = processXform(f, xform, it.id);
                            reporter.modifyContent(f);
                            if (result == null) {
                                session.remove(f);
                                return;
                            } else {
                                List urlList = it.targetPath instanceof List ? it.targetPath : [it.targetPath];
                                transferResult(result, sync, urlList, it);
                            }
                        } catch (Exception ex1) {
                            trace("!!!!!!!Exception: ${ex1.toString()}");
                            StringBuilder exMsgBldr = new StringBuilder();
                            exMsgBldr.append("Exception '${ex1.toString()}' occurs");
                            exMsgBldr.append(" while processing FlowFile '${f.getAttribute('filename')}'");

                            exMsgBldr.append(" in '${f.getAttribute('business.process.name')}' scenario");
                            exMsgBldr.append(" at '${it.id}' target");
                            exMsgBldr.append(" at ${f.getAttribute('xform.path')} path");
                            exMsgBldr.append(" at ${f.getAttribute('xform.stage')} stage");

                            logger.log(org.apache.nifi.logging.LogLevel.ERROR, exMsgBldr.toString());
                            logger.log(org.apache.nifi.logging.LogLevel.ERROR, traceOut);

                            session.putAttribute(f, "error.msg", ex1.toString());

                            //failFlowFiles << flowFileCopy

                            session.transfer(f, Failure);
                        }
                    }
                }
            }


        } catch (Exception ex) {

        }

        final DistributedMapCacheClient cache = context.getProperty(PROP_DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        session.transfer(flowFile, Load);

//        var lookup = context.;
    }

    /**Не до конца понимает, что должно вернуть**/
    private ControllerService getServiceController (final String name, final ProcessContext context){
        trace(String.format("get service controller: %s", name));
        var lookup = context.getControllerServiceLookup();
        String serviceId = lookup.getControllerServiceIdentifiers(ControllerService.class)
                .stream()
                .filter(cs -> lookup.getControllerServiceName(cs).equals(name))
                .findFirst()
                .orElse(null);
        return lookup.getControllerService(serviceId);
    }

    //старая версия листа
//    List<Object> evaluateXPath(InputStream inputStream, String xpathQuery){
//        Element records = null;
//        try {
//            records = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream).getDocumentElement();
//        } catch (SAXException | IOException | ParserConfigurationException e) {
//            throw new RuntimeException(e);
//        }
//        XPath xPath = XPathFactory.newInstance().newXPath();
//        Object nodes;
//        try {
//            nodes = xPath.evaluate(xpathQuery, records, XPathConstants.NODESET);
//        } catch (XPathExpressionException e) {
//            throw new RuntimeException(e);
//        }
//        return DefaultGroovyMethods.collect{node -> node.textContent};


    public List<String> evaluateXPath(InputStream inputStream, String xpathQuery) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(inputStream);

        XPathFactory xPathFactory = XPathFactory.newInstance();
        XPath xpath = xPathFactory.newXPath();


        NodeList nodes = (NodeList) xpath.evaluate(xpathQuery, document.getDocumentElement(), XPathConstants.NODESET);

        List<String> result = new ArrayList<>();
        for (int i = 0; i < nodes.getLength(); i++) {
            result.add(nodes.item(i).getTextContent());
        }
        return result;
    }

    //Не уверен в правильности из-за кучи try catch
    public Object evaluateXPathValue(InputStream inputStream, String xpathQuery){
        Element records = null;
        try {
            records = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream).getDocumentElement();
        } catch (SAXException | IOException | ParserConfigurationException e) {
            throw new RuntimeException(e);
        }
        XPath xPath = XPathFactory.newInstance().newXPath();
        String res = null;
        try {
            res = xPath.evaluate(xpathQuery, records);
        } catch (XPathExpressionException e) {
            throw new RuntimeException(e);
        }
        return res;
    }

    //Вроде норм, но надо будет потестить
    private String stream2string (InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        for (int length; (length = inputStream.read(buffer)) != -1; ) {
            result.write(buffer, 0, length);
        }
        try {
            inputStream.reset();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            return result.toString("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    //Вроде норм, но надо будет потестить
    private byte[] stream2byteArray (InputStream inputStream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        for (int length; (length = inputStream.read(buffer)) != -1; ) {
            result.write(buffer, 0, length);
        }
        return result.toByteArray();
    }

    //Сделано не как в груви, но должно работать
    private String xml2Json(String xml){
        JSONObject jsonObj = XML.toJSONObject(xml);
        String json = jsonObj.toString();
        return json;
    }

    //доделать
    FlowFile convertFlowFile(FlowFile flowFile) throws Exception{
        flowFile = session.write(flowFile, new StreamCallback(){

            @Override
            public void process(InputStream is, OutputStream os) throws IOException{
                byte[] content = stream2byteArray(is);
                trace "Len" + content.length
                String json = xml2Json(new String(content));
                if (!json.isEmpty()) {
                    os.write(json.getBytes());
                    os.flush();
                } else {
                    throw new Exception("Failed xml convertation!");
                }
            }
        });
        return flowFile;
    }


    public void trace(String message) {
        traceOut += String.format("\r\n+++++++ %s +++++++:%d",traceCount, message);
    }

    private void trace1(String message) {
        traceOut1 += String.format("\r\n+++++++ %s +++++++:%d",traceCount1, message);
    }

    private String getResponse(String protocol, String code) {
        if (protocol.equals("XI")) {
            switch (code) {
                case "200":
                    return "XI_OK";
            }
        } else {
            return code;
        }
        return null;
    }

    private int findTarget(JSONArray targets, String id) {
        JSONObject target;
        for (int i = 0; i < targets.length(); i++) {
            target = targets.getJSONObject(i);
            if(target.get("id") == id) {
                return i;
            }
        }
        return -1;
    }
}
