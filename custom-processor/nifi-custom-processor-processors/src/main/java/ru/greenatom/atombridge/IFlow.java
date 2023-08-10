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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;

import java.io.InputStream;
import java.io.StringReader;
import java.util.*;

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
                    if (targetIndx < 0) {
                        throw new IllegalArgumentException("Target not found");
                    }
                    JSONObject target = targets.getJSONObject(targetIndx);

                    JSONArray xforms = iflow.getJSONArray("xforms");

                    int xformPath = Integer.parseInt(flowFile.getAttribute("xform.path"));

                    if (xformPath > -1 & xformPath < xforms.length()) {
                        if (target.get("output") =="JSON") {
                            session.putAttribute(flowFile, "target.output", "JSON");
                        }
                        var xform = xforms.get(xformPath);
                        FlowFile result = processXform(context, session, flowFile, xform, targetId);
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
                    exMsgBldr.append("Exception" + ex1 + "occurs");
                    exMsgBldr.append(" while processing FlowFile" + flowFile.getAttribute("filename"));

                    exMsgBldr.append(" in " + flowFile.getAttribute("business.process.name") + "scenario");
                    exMsgBldr.append(" at " + flowFile.getAttribute("target.id") + "target");
                    exMsgBldr.append(" at " + flowFile.getAttribute("xform.path") + "path");
                    exMsgBldr.append(" at " + flowFile.getAttribute("xform.stage") +  "stage");

                    session.putAttribute(flowFile, "error.msg", ex1.toString());

                    logger.log(org.apache.nifi.logging.LogLevel.ERROR, exMsgBldr.toString());
                    logger.log(org.apache.nifi.logging.LogLevel.ERROR, traceOut);

                    session.putAttribute(flowFile, "error.msg", ex1.toString());

                    //failFlowFiles << flowFileCopy
                    session.transfer(flowFile, Failure);
                    return;
                }
            } else {
                //Validate against xsd schema
                session.putAttribute(flowFile, "xform.stage", "-1");
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
                                        return null;
                                    }
                                    return new String(value, StandardCharsets.UTF_8);

                                }});
                    if (schemaContent == null) {
                        throw new IOException("Schema with name ${iflow.validate} not found");
                    }
                } catch (Exception e) {
                    //todo Мб вынести в отдельный метод?
                    String msg = "Failed schema extraction! " + e;
                    setError(session, msg, flowFile);
                    isFailedSchemaExtraction = true;
                }
                if (isFailedSchemaExtraction) {
                    return;
                }
                InputStream fis = null;
                boolean isFailedValidation = false;
                try {
                    fis = session.read(flowFile);
                    Source xmlFile = new StreamSource(fis);
                    javax.xml.validation.SchemaFactory schemaFactory = SchemaFactory
                            .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                    javax.xml.validation.Schema schema = schemaFactory.newSchema(new StreamSource(new StringReader(schemaContent)))
                    javax.xml.validation.Validator validator = schema.newValidator();
                    validator.validate(xmlFile);
                } catch (Exception e) {
                    String msg = "Failed xml validation! " + e;
                    logger.error(msg);
                    setError(session, msg, flowFile);
                    isFailedValidation = true;
                } finally {
                    fis.close();
                }
                if (isFailedValidation) {
                    return;
                }

                ProvenanceReporter reporter = session.getProvenanceReporter();

                targets.eachWithIndex { it, flowIndex ->
                        ArrayList xforms = it.transformations as ArrayList
                    //Make a copy of incoming flow file for each target system
                    //Or use the incoming flowfile for last target
                    //FlowFile file = flowIndex < numOfTargets - 1 & numOfTargets > 1 ? session.clone(flowFile) : flowFile
                    FlowFile file = null;
                    if(flowIndex < numOfTargets - 1 & numOfTargets > 1){
                        file = session.clone(flowFile);
                        reporter.clone(flowFile, file);
                    } else{
                        file = flowFile;
                    }
                    session.putAttribute(file, "Receiver", it.id);
                    int xformPath = -1;

                    if (it.syncValidation == "true") {
                        syncResponse(session, file);
                    }

                    if (it.output == "JSON") {
                        session.putAttribute(file, "target.output", "JSON")
                    }

                    FlowFile f = null;

                    for (ArrayList xform : xforms) {
                        try {
                            xformPath++;
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

    //Returns processed FlowFile or ArrayList of processed FlowFiles
    private FlowFile processXform(
            final ProcessContext context,
            final ProcessSession session,
            FlowFile flowFile,
            JSONArray xforms,
            String targetId
    ) throws Exception{
        boolean isFlowFileSuppressed = false;
        int prevStageIndx = -1;

        session.putAttribute(flowFile, "target.id", targetId);

        //If a flow file processed previosly, start right after the previous stage
        //todo Условие перепроверить, поставил на первое время..
        if (flowFile.getAttribute("xform.stage") != null) {
            prevStageIndx = Integer.parseInt(flowFile.getAttribute("xform.stage"));
        } else {
            session.putAttribute(flowFile, "xform.stage", "0");
        }
        boolean isPropagated = false;

        //Processing transformation for a target system
        int currStageIndx = -1;
        trace1("ID " + targetId);
        trace1("prev stage " + prevStageIndx);

        trace1(" " + xforms);
        session.putAttribute(flowFile, "xform.stage", "0");

        session.getProvenanceReporter().modifyContent(flowFile, "wsrhsrh");

        for (int i = 0; i < xforms.length(); i++) {
            JSONObject xform = xforms.getJSONObject(i);
            //Stop processing flow file if it is suppressed at routing stage
            if (isFlowFileSuppressed) {
                return null;
            }
            currStageIndx++;
            session.putAttribute(flowFile, "xform.stage", String.valueOf(currStageIndx));

            //Start process right after the previous stage
            if (currStageIndx > prevStageIndx) {
                trace1("Stage " + String.valueOf(currStageIndx));
                //TODO при дебаге проверить получившийся JSON
                String[] nameParamsPair = xform.toString().split("://");
                Map<String, String> params = null;
                if (nameParamsPair.length > 1) {
                    //Params must follow after ? symbol
                    params = parseParams(nameParamsPair[1].substring(1));
                }
                for (Map.Entry<String, String> paramEntry : params.entrySet()) {
                    trace("Key " + paramEntry.getKey() + " val " + paramEntry.getValue());
                }
                //TODO временно оставил toString()
                String name = nameParamsPair.length > 1 ? nameParamsPair[0] : xform.toString();
                trace("processing " + String.valueOf(currStageIndx) + " stage");

                PropertyValue propValue;
                String param;
                switch (name) {
                    case("SyncResponse"):
                        syncResponse(session, flowFile);
                        break;
                    case("RouteOnContent"):
                        //Have to transfer a flow file to the special RouteOnContent processor group
                        param = params.get("MatchRequirement");
//                        if (!param) throw new IllegalArgumentException(name + ' ' + param);
                        checkParam(param, name);
                        session.putAttribute(flowFile, "content.match.strategy", param);
                        param = params.get("RouteCondition");
//                        if (!param) throw new IllegalArgumentException(name + ' ' + param);
                        checkParam(param, name);
                        propValue = context.newPropertyValue(param);
                        String s = propValue.evaluateAttributeExpressions(flowFile).getValue();
                        session.putAttribute(flowFile, "route.on.content.condition", s);
                        param = params.get("Result");
                        propValue = context.newPropertyValue(param);
                        s = propValue.evaluateAttributeExpressions(flowFile).getValue();
                        session.putAttribute(flowFile, "route.on.content.result", s);
                        session.putAttribute(flowFile, "xform.group", "RouteOnContent");
                        isPropagated = true;
                        break;
                    case("UpdateAttribute"):
                        //We have to support the Nifi EL in attributes
                        //So create temp hidden property to provide EL capabilities
                        for (Map.Entry<String, String> entry : params.entrySet()) {
                            propValue = context.newPropertyValue(entry.getValue());
                            String attrValue = propValue.evaluateAttributeExpressions(flowFile).getValue();
                            flowFile = session.putAttribute(flowFile, entry.getKey(), attrValue);
                        }
                        break;
                    case("RouteOnAttribute"):
                        param = params.get("RoutingStrategy");
//                        if (!param) throw new IllegalArgumentException(name + ' ' + param);
                        checkParam(param, name);
                        param = params.get("Condition");
//                        if (!param) throw new IllegalArgumentException(name + ' ' + param);
                        checkParam(param, name);
                        propValue = context.newPropertyValue(param);
                        String res = propValue.evaluateAttributeExpressions(flowFile).getValue();
                        if (res == "false") {
                            isFlowFileSuppressed = true;
                            //был в изначальном скрипте: throw new Exception('Result ' + res + ' does not match condition')
                        }
                        break;
                    case("ReplaceText"):
                        //Have to transfer a flow file to the special ReplaceText processor group
                        //TODO не совсем понятно про param здесь, в исходном скрипте он подсвечем серым
                        //TODO Имелось в виду replacementStrategy вместо param или так и задуманно?
                        String replacementStrategy = params.get("ReplacementStrategy");
//                        if (!replacementStrategy) throw new IllegalArgumentException(name + ' ' + param);
                        checkParam(replacementStrategy, name);
                        session.putAttribute(flowFile, "replace.text.mode", replacementStrategy);
                        String searchValue = params.get("SearchValue");
//                        if (!searchValue) throw new IllegalArgumentException(name + ' ' + param);
                        checkParam(searchValue, name);
                        propValue = context.newPropertyValue(searchValue);
                        searchValue = propValue.evaluateAttributeExpressions(flowFile).getValue();
                        session.putAttribute(flowFile, "replace.text.search.value", searchValue);
                        String replacementValue = params.get("ReplacementValue");
//                        if (!replacementValue) throw new IllegalArgumentException(name + ' ' + param);
                        checkParam(replacementValue, name);
                        propValue = context.newPropertyValue(replacementValue);
                        replacementValue = propValue.evaluateAttributeExpressions(flowFile).getValue();
                        session.putAttribute(flowFile, "replace.text.replacement.value", replacementValue);
                        final int fileSize = (int) flowFile.getSize();
                        flowFile = replaceText(flowFile, replacementStrategy, searchValue,
                                replacementValue, "EntireText", StandardCharsets.UTF_8, fileSize);
                        break;
                    case("EvaluateXQuery"):
                        param = params.get("Destination");
                        if (!param.equals("flowfile-attribute")) {
                            throw new IllegalArgumentException(name + ' ' + param);
                        }
                        params.remove("Destination");
                        for (Map.Entry<String, String> paramEntry : params.entrySet()) {
                            trace("Processing ${paramEntry.getKey()} ");

                            if (paramEntry.getValue().indexOf("count(") > -1) {
                                trace("+count");
                                final StringBuilder sb = new StringBuilder();
                                session.read(flowFile, new InputStreamCallback(){

                                    @Override
                                    public void process(InputStream is) throws IOException{
                                        var r = evaluateXPathValue(is,
                                                paramEntry.getValue().replace("\\\\", "\\"));
                                        sb.append(r);
                                    }

                                });
                                session.putAttribute(flowFile, paramEntry.getKey(), sb.toString());
                            } else {
                                //final ByteArrayOutputStream baos = new ByteArrayOutputStream()
                                final List<String> list = new ArrayList<>();

                                session.read(flowFile, new InputStreamCallback(){

                                    @Override
                                    public void process(InputStream is) throws IOException{
                                        List<Object> nodes = evaluateXPath(is,
                                                paramEntry.getValue().replace("\\\\", "\\"));

                                        for (Object node : nodes) {
                                            list.add(node);
                                        }

                                    }

                                });

                                //res = baos.toString()
                                trace1("+res");
                                if (list.size() == 1) {
                                    session.putAttribute(flowFile, paramEntry.getKey(), list.get(0));
                                    trace1("EvalXq res ${paramEntry.getKey()} " + list.get(0));
                                } else {
                                    int sfx = 1;
                                    for (String s : list) {
                                        String attrName = paramEntry.getKey() + '.' + String.valueOf(sfx);
                                        trace1("EvalXq res ${attrName} " + s);
                                        session.putAttribute(flowFile, attrName, s);
                                        sfx++;
                                    }
                                }
                            }

                            //String r = Arrays.toString(res)
                        }
                        break;
                    case("ApplyXslt"):
                        final String parameter = params.get("Name");
                        checkParam(parameter, name);
//                        if (!param) throw new IllegalArgumentException(name + ' ' + param);
                        flowFile = session.write(flowFile, new StreamCallback(){

                            @Override
                            public void process(InputStream is, OutputStream os) throws IOException{
                                applyXslt(is, os, parameter);
                                os.flush();
                            }

                        });
                        break;
                    case("DuplicateFlowFile"):
                        param = params.get("Number");
                        checkParam(param, name);
//                        if (param == null) {
//                            throw new IllegalArgumentException(name + ' ' + param);
//                        }
                        propValue = context.newPropertyValue(param);
                        param = propValue.evaluateAttributeExpressions(flowFile).getValue();
                        int numOfCopies = Integer.parseInt(param);
                        //res -> result
                        ArrayList<FlowFile> result = new ArrayList<>();

                        session.putAttribute(flowFile, "copy.index", "0");

                        if (currStageIndx == xforms.length() - 1) {
                            flowFile = session.removeAttribute(f, "xform.group");
                        }
                        String ffid = flowFile.getAttribute("uuid");
                        for (int j = 0; j < numOfCopies; j++) {
                            FlowFile f = session.clone(flowFile);
                            session.putAttribute(flowFile, "copy.index", String.valueOf(j + 1));
                            graylogNotifyStart(f, ffid);
                            FlowFile ff = null;
                            if (currStageIndx < xforms.length() - 1) {
                                ff = processXform(context, session, f, xforms, targetId);
                            }
                            if (ff == null) {
                                session.remove(f);
                            } else {
                                result.add(ff);
                            }
                        }
                        if (currStageIndx < xforms.length() - 1) {
                            FlowFile ff = processXform(context, session, flowFile, xforms, targetId);
                            if (ff == null) {
                                session.remove(flowFile);
                            } else {
                                result.add(ff);
                            }
                        } else {
                            result.add(flowFile);
                        }
                        return result;
                    default:
                        for (Map.Entry<String, String> entry : params.entrySet()) {
                            flowFile = session.putAttribute(flowFile,
                                    entry.getKey(), entry.getValue());
                        }
                        session.putAttribute(flowFile, "xform.group", name);
                        break;
                }
                graylogNotify(flowFile, name);
            }
            if (isPropagated) {
                break;
            }
        }
        trace("Stage is " + String.valueOf(currStageIndx) + " size " + String.valueOf(xforms.length()));
        if (currStageIndx == xforms.length() - 1) {
            if (isPropagated) {
                if (!flowFile.getAttribute("target.output").equals("JSON")) {
                    //Red pill for last xform to transfer flow file to the upload group
                    session.putAttribute(flowFile, "xform.last", "true");
                }
            } else {
                if (flowFile.getAttribute("target.output").equals("JSON")) {
                    flowFile = convertFlowFile(session, flowFile);
                }
                //Move right to upload group
                flowFile = session.removeAttribute(flowFile, "xform.group");
            }
        }

        trace1("FF stage " + flowFile.getAttribute("xform.stage"));
        return flowFile;
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

    private void checkParam(String param, String name) {
        if (param == null) {
            throw new IllegalArgumentException(name + ' ' + param);
        }
    }

    private Map<String, String> parseParams(String url) throws Exception {
        Map<String, String> params = new HashMap<>();
        String[] keyValuePairs = url.split("&");

        for (String pair : keyValuePairs) {
            String[] keyValuePair = pair.split("=");
            if (keyValuePair.length > 0) params.put(keyValuePair[0].trim(), keyValuePair[1].trim());
        }
        return params;
    }

    private FlowFile convertFlowFile(final ProcessSession session, FlowFile flowFile) throws Exception{
        flowFile = session.write(flowFile, new StreamCallback(){

            @Override
            public void process(InputStream is, OutputStream os) throws IOException{
                byte[] content = stream2byteArray(is);
                trace("Len " + content.length);
                String json = xml2Json(new String(content));
                if (json != null) {
                    os.write(json.getBytes());
                    os.flush();
                } else {
                    try {
                        throw new Exception("Failed xml convertation!");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }

        });
        return flowFile;
    }

    private FlowFile regexReplaceText(
            final ProcessSession session,
            final FlowFile flowFile,
            final String searchValue,
            final String replacementValue,
            final String evaluateMode,
            final Charset charset,
            final int maxBufferSize
    ) throws Exception {
        final int numCapturingGroups = Pattern.compile(searchValue).matcher("").groupCount();
        //final AttributeValueDecorator quotedAttributeDecorator = Pattern::quote

        final String searchRegex = searchValue;
        final Pattern searchPattern = Pattern.compile(searchRegex);
        final Map<String, String> additionalAttrs = new HashMap<>(numCapturingGroups);

        FlowFile updatedFlowFile;
        if (evaluateMode.equalsIgnoreCase("EntireText")) {
            final int flowFileSize = (int) flowFile.getSize();
            final int bufferSize = Math.min(maxBufferSize, flowFileSize);
            final byte[] buffer = new byte[bufferSize];

            session.read(flowFile,
                    new InputStreamCallback() {

                        @Override
                        public void process(InputStream is) throws IOException {
                            StreamUtils.fillBuffer(is, buffer, false);

                        }
                    });

            final String contentString = new String(buffer, 0, flowFileSize, charset);
            final Matcher matcher = searchPattern.matcher(contentString);

            //final PropertyValue replacementValueProperty = replacementValue

            int matches = 0;
            final StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                matches++;

                for (int i = 0; i <= matcher.groupCount(); i++) {
                    additionalAttrs.put("$" + i, matcher.group(i));
                }
                String replacementFinal = normalizeReplacementString(replacementValue);

                matcher.appendReplacement(sb, replacementFinal);
            }

            if (matches > 0) {
                matcher.appendTail(sb);

                final String updatedValue = sb.toString();
                updatedFlowFile = session.write(flowFile, new OutputStreamCallback() {

                    @Override
                    public void process(OutputStream os) throws IOException {
                        os.write(updatedValue.getBytes(charset));

                    }
                });
            } else {
                return flowFile;
            }
        } else throw new Exception("unsupported evaluation mode");
        return updatedFlowFile;
    }

    private static String normalizeReplacementString(String replacement) {
        String replacementFinal = replacement;
        if (Pattern.compile("(\\$\\D)").matcher(replacement).find()) {
            replacementFinal = Matcher.quoteReplacement(replacement);
        }
        return replacementFinal;
    }

    FlowFile replaceText(
            final ProcessSession session,
            final FlowFile flowFile,
            final String type,
            final String searchValue,
            final String replacementValue,
            final String evaluateMode,
            final Charset charset,
            final int maxBufferSize
    ) throws Exception{
        if (type.equals("RegexReplace")) {
            return regexReplaceText(session, flowFile, searchValue, replacementValue, evaluateMode, charset, maxBufferSize);
        } else {
            throw new Exception("Incorrect replace strategy");
        }
    }

    private void setError(
            final ProcessSession session,
            String msg, FlowFile flowFile
    ) {
        logger.error(msg);
        session.putAttribute(flowFile, "error.msg", msg);
        session.transfer(flowFile, Failure);
    }

    private void syncResponse(
            final ProcessSession session,
            FlowFile flowFile
    ) {
        FlowFile syncResponseFile = session.create(flowFile);
        session.putAttribute(syncResponseFile, "sync.response", "true");
        //todo Тут было отношение REL_SUCCESS
        session.transfer(syncResponseFile, Transform);
    }

}
