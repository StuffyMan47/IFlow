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
import org.w3c.dom.Element;
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
            var iflow = new groovy.json.JsonSlurper().parseText(ret);

            ArrayList<Object> targets = iflow.targets as ArrayList;
            trace("full list of defined target systems" + targets);

        } catch (Exception ex) {

        }

        final DistributedMapCacheClient cache = context.getProperty(PROP_DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

        session.transfer(flowFile, Load);

//        var lookup = context.;
    }

    /**Не до конца понимает, что должно вернуть**/
    public ControllerService getServiceController (final String name, final ProcessContext context){
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
//
//    }
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

//    def stream2string(InputStream inputStream) {
//        ByteArrayOutputStream result = new ByteArrayOutputStream()
//        byte[] buffer = new byte[1024]
//        for (int length; (length = inputStream.read(buffer)) != -1; ) {
//            result.write(buffer, 0, length)
//        }
//        // StandardCharsets.UTF_8.name() > JDK 7
//        inputStream.reset()
//        return result.toString('UTF-8')
//    }

    public void trace(String message) {
        traceOut += String.format("\r\n+++++++ %s +++++++:%d",traceCount, message);
    }

    public void trace1(String message) {
        traceOut1 += String.format("\r\n+++++++ %s +++++++:%d",traceCount1, message);
    }

    public String getResponse(String protocol, String code) {
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
}
