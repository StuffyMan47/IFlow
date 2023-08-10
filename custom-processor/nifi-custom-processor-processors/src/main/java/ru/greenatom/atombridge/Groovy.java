import groovy.lang.Binding;
import groovy.lang.Closure;
import groovy.lang.Reference;
import groovy.lang.Script;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.stream.io.StreamUtils;
import org.codehaus.groovy.runtime.DefaultGroovyMethods;
import org.codehaus.groovy.runtime.StringGroovyMethods;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Groovy extends Script {
    public static void main(String[] args) {
        new Groovy(new Binding(args)).run();
    }

    public Object run() {


        setProperty("session", (ProcessSession) this.getBinding().getProperty("session"));

        final FlowFile flowFile = this.getBinding().getProperty("session").invokeMethod("get", new Object[0]);
        if (!flowFile.asBoolean()) return;


        setProperty("xslRemoveEnv", null);
//List targetsRestrictList
//List flowFiles = [] as List<FlowFile>
//List failFlowFiles = [] as List<FlowFile>

        setProperty("traceOut", "");
        setProperty("traceCount", 0);
        setProperty("traceOut1", "");
        setProperty("traceCount1", 0);

        setProperty("xpath", XPathFactory.newInstance().newXPath());
        setProperty("builder", DocumentBuilderFactory.newInstance().newDocumentBuilder());

        String iflowMapCacheLookupClientName = this.getBinding().getProperty("IflowMapCacheLookupClient").value;
        String xsdMapCacheLookupClientName = this.getBinding().getProperty("XsdMapCacheLookupClient").value;
        String xsltMapCacheLookupClientName = this.getBinding().getProperty("XsltMapCacheLookupClient").value;
//iflowCacheMap = null
//xsdCacheMap = null
//xsltCacheMap = null

        try {
            setProperty("iflowCacheMap", getServiceController(iflowMapCacheLookupClientName));
            setProperty("xsdCacheMap", getServiceController(xsdMapCacheLookupClientName));
            setProperty("xsltCacheMap", getServiceController(xsltMapCacheLookupClientName));

//iflowCacheMap = CTL.iflowMapCacheLookup
//xsdCacheMap = CTL.xsdMapLookup
//xsltCacheMap = CTL.xsltMapCacheLookup

            //trace("step 1: get iFlow config from cache: ${flowFile.getAttribute('business.scenario')}")

            // Optional<String> ret = iflowCacheMap.lookup(["key": flowFile.flow_name])
            //Map<String, String> creds = new HashMap<String, String>()
            //creds.put("key", flowFile.flow_name)
            //Optional<String> ret = iflowCacheMap.get(flowFile.flow_name,
            String ret = DefaultGroovyMethods.invokeMethod(this.getBinding().getProperty("iflowCacheMap"), "get", new Object[]{flowFile.invokeMethod("getAttribute", new Object[]{"business.process.name"}), new Serializer<String>() {
                @Override
                public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
                    out.write(value.getBytes(StandardCharsets.UTF_8));
                }

            }, new Deserializer<String>() {
                @Override
                public String deserialize(final Byte[] value) throws DeserializationException, IOException {
                    if (value == null) {
                        return null;
                    }

                    return new String(value, StandardCharsets.UTF_8);

                }

            }});
            if (!StringGroovyMethods.asBoolean(ret)) {
                trace("iFlow not found, return 501");
                this.getBinding().getProperty("log").invokeMethod("error", new Object[]{"iFlow named:" + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{flowFile.invokeMethod("getAttribute", new Object[]{"business.process.name"})}) + " not found!"});
                flowFile.iflow.error = "iFlow named:" + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{flowFile.invokeMethod("getAttribute", new Object[]{"business.process.name"})}) + " not found!";
                flowFile.iflow.status.code = getResponse("", "501");
                this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{flowFile, this.getBinding().getProperty("REL_FAILURE")});
                return;

            } else {
                //trace("readed iFlow config:\r\n${ret.get()}")
                trace("readed iFlow config");
            }

            //    def iflow = flowFile.flow_name.endWIth(".json") ? new groovy.json.JsonSlurper().parseText(ret.get()) : new groovy.yaml.YamlSlurper().parseText(ret.get())
            trace("start parsing json iFlow");
            final Object iflow = new groovy.json.JsonSlurper().invokeMethod("parseText", new Object[]{ret});
            //    if(iflow.trace){}
            //read flowfile content into memory:
    /*InputStream is = flowFile.read()
    ByteArrayInputStream fc = stream2byteArray(is)
    is.close()*/

            //if (iflow.input == "SOAP")
            //     fc = applyTransform(fc, "RemoveEnvelope.xsl")
            //    trace("After remove envelope " + fc.available())
            //log.info "\r\nstart transform"
            //    if (iflow.transform)
            //        fc = applyTransform(fc, iflow.transform)
    /*   if (iflow.transform) {
           iflow.transform.each {
               fc = applyTransform(fc, it)
           }
   }    else
           fc = applyTransform(fc, iflow.transform)
*/
    /*if (iflow.targetsRestrict) {
        trace "evaluate restrict for target systems: ${iflow.targetsRestrict}"
        if (iflow.input == 'SOAP' || iflow.input == 'XML') {
            targetsRestrictList = evaluateXPath(fc, iflow.targetsRestrict) as String[]
        }
       else if (iflow.input == 'JSON')
           log.error('JSON target restrict TODO:' + iflow)
        trace "targetsRestrictList: ${targetsRestrictList}"
    }*/

            final ArrayList targets = DefaultGroovyMethods.asType(iflow.targets, ArrayList.class);
            trace("full list of defined target systems: " + String.valueOf(targets));
            //int lastIndex = targets.size() - 1
            //   ByteArrayInputStream fcCopy
            //if(lastIndex > 0) {
            //      fcCopy = stream2byteArray(fc)
            //      fc.reset()
            //  }

            final boolean sync = Boolean.parseBoolean(iflow.sync);

            final int numOfTargets = targets.size();
            if (DefaultGroovyMethods.and(DefaultGroovyMethods.and(flowFile.invokeMethod("getAttribute", new Object[]{"xform.stage"}) != null, flowFile.invokeMethod("getAttribute", new Object[]{"target.id"}) != null), flowFile.invokeMethod("getAttribute", new Object[]{"xform.path"}) != null)) {
                try {
                    trace1("+loopback " + flowFile.invokeMethod("getAttribute", new Object[]{"xform.stage"}));
                    //trace1 '+loopback ' + flowFile.getAttribute('xform.stage') + ' ' flowFile.getAttribute('target.id')
                    String targetId = flowFile.invokeMethod("getAttribute", new Object[]{"target.id"});
                    int targetIndx = findTarget(targets, targetId);
                    if (targetIndx < 0) throw new IllegalArgumentException("Target not found");
                    Object target = targets.get(targetIndx);

                    ArrayList xforms = DefaultGroovyMethods.asType(target.transformations, ArrayList.class);

                    int xformPath = Integer.parseInt(flowFile.invokeMethod("getAttribute", new Object[]{"xform.path"}));

                    if (DefaultGroovyMethods.and(xformPath > -1, xformPath < xforms.size())) {
                        if (target.output.equals("JSON"))
                            flowFile.invokeMethod("putAttribute", new Object[]{"target.output", "JSON"});
                        Object xform = xforms.get(xformPath);
                        Object result = processXform(flowFile, xform, targetId);
                        if (result == null) {
                            trace("-ff");
                            return this.getBinding().getProperty("session").invokeMethod("remove", new Object[]{flowFile});
                        } else {
                            List urlList = target.targetPath instanceof List ? target.targetPath : new ArrayList(Arrays.asList(target.targetPath));
                            transferResult(result, sync, urlList, target);
                        }

                    } else {
                        throw new Exception("Incorrect transformation path " + xformPath);
                    }

                    //log.log(org.apache.nifi.logging.LogLevel.ERROR, traceOut1)
                } catch (Exception ex1) {
                    trace("!!!!!!!Exception: " + ex1.toString());
                    StringBuilder exMsgBldr = new StringBuilder();
                    exMsgBldr.append("Exception '" + ex1.toString() + "' occurs");
                    exMsgBldr.append(" while processing FlowFile '" + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{flowFile.invokeMethod("getAttribute", new Object[]{"filename"})}) + "'");

                    exMsgBldr.append(" in '" + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{flowFile.invokeMethod("getAttribute", new Object[]{"business.process.name"})}) + "' scenario");
                    exMsgBldr.append(" at '" + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{flowFile.invokeMethod("getAttribute", new Object[]{"target.id"})}) + "' target");
                    exMsgBldr.append(" at '" + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{flowFile.invokeMethod("getAttribute", new Object[]{"xform.path"})}) + "' path");
                    exMsgBldr.append(" at " + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{flowFile.invokeMethod("getAttribute", new Object[]{"xform.stage"})}) + " stage");

                    this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, "error.msg", ex1.toString()});

                    this.getBinding().getProperty("log").invokeMethod("log", new Object[]{LogLevel.ERROR, exMsgBldr.toString()});
                    this.getBinding().getProperty("log").invokeMethod("log", new Object[]{LogLevel.ERROR, this.getBinding().getProperty("traceOut")});

                    this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, "error.msg", ex1.toString()});

                    //failFlowFiles << flowFileCopy
                    this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{flowFile, this.getBinding().getProperty("REL_FAILURE")});
                    return;

                }

            } else {
                //Validate against xsd schema
                flowFile.xform.stage = -1;
                String schemaContent = null;
                boolean isFailedSchemaExtraction = false;
                try {
                    schemaContent = ((String) (DefaultGroovyMethods.invokeMethod(this.getBinding().getProperty("xsdCacheMap"), "get", new Object[]{iflow.validate, new Serializer<String>() {
                        @Override
                        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
                            out.write(value.getBytes(StandardCharsets.UTF_8));
                        }

                    }, new Deserializer<String>() {
                        @Override
                        public String deserialize(final Byte[] value) throws DeserializationException, IOException {
                            if (value == null) {
                                return null;
                            }

                            return new String(value, StandardCharsets.UTF_8);

                        }

                    }})));
                    if (!StringGroovyMethods.asBoolean(schemaContent)) {
                        throw new IOException("Schema with name " + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{iflow.validate}) + " not found");
                    }

                } catch (Exception e) {
                    String msg = "Failed schema extraction! " + e;
                    this.getBinding().getProperty("log").invokeMethod("error", new Object[]{msg});
                    flowFile.error.msg = msg;
                    this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{flowFile, this.getBinding().getProperty("REL_FAILURE")});
                    isFailedSchemaExtraction = true;
                }

                if (isFailedSchemaExtraction) return;


                InputStream fis = null;
                boolean isFailedValidation = false;
                try {
                    fis = ((InputStream) (this.getBinding().getProperty("session").invokeMethod("read", new Object[]{flowFile})));
                    Source xmlFile = new StreamSource(fis);
                    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                    Schema schema = schemaFactory.newSchema(new StreamSource(new StringReader(schemaContent)));
                    Validator validator = schema.newValidator();
                    validator.validate(xmlFile);
                } catch (Exception e) {
                    String msg = "Failed xml validation! " + e;
                    this.getBinding().getProperty("log").invokeMethod("error", new Object[]{msg});
                    flowFile.error.msg = msg;
                    isFailedValidation = true;
                    this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{flowFile, this.getBinding().getProperty("REL_FAILURE")});
                } finally {
                    fis.close();
                }

                if (isFailedValidation) return;


                final ProvenanceReporter reporter = this.getBinding().getProperty("session").invokeMethod("getProvenanceReporter", new Object[0]);

                return DefaultGroovyMethods.eachWithIndex(targets, new Closure(this, this) {
                    public Object doCall(final Object it, Object flowIndex) {
                        ArrayList xforms = DefaultGroovyMethods.asType(it.transformations, ArrayList.class);
                        //Make a copy of incoming flow file for each target system
                        //Or use the incoming flowfile for last target
                        //FlowFile file = flowIndex < numOfTargets - 1 & numOfTargets > 1 ? session.clone(flowFile) : flowFile
                        FlowFile file = null;
                        if (DefaultGroovyMethods.and(flowIndex < numOfTargets - 1, numOfTargets > 1)) {
                            file = ((FlowFile) (Groovy.this.getBinding().getProperty("session").invokeMethod("clone", new Object[]{flowFile})));
                            reporter.clone(flowFile, file);
                        } else {
                            file = flowFile;
                        }

                        Groovy.this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{file, "Receiver", it.id});
                        int xformPath = -1;

                        if (it.syncValidation.equals("true")) {
                            syncResponse(file);
                        }


                        if (it.output.equals("JSON")) {
                            Groovy.this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{file, "target.output", "JSON"});
                        }


                        final Reference<FlowFile> f = new Reference<FlowFile>(null);

                        for (ArrayList xform : xforms) {
                            try {
                                xformPath++;
                                Groovy.this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{file, "xform.path", String.valueOf(xformPath)});
                                f.set(DefaultGroovyMethods.and(xformPath < xforms.size() - 1, xforms.size() > 1) ? Groovy.this.getBinding().getProperty("session").invokeMethod("clone", new Object[]{file}) : file);

                                Object result = processXform(f.get(), xform, it.id);
                                reporter.modifyContent(f.get());
                                if (result == null) {
                                    Groovy.this.getBinding().getProperty("session").invokeMethod("remove", new Object[]{f.get()});
                                    return;

                                } else {
                                    List urlList = it.targetPath instanceof List ? it.targetPath : new ArrayList(Arrays.asList(it.targetPath));
                                    transferResult(result, sync, urlList, it);
                                }

                            } catch (Exception ex1) {
                                trace("!!!!!!!Exception: " + ex1.toString());
                                StringBuilder exMsgBldr = new StringBuilder();
                                exMsgBldr.append("Exception '" + ex1.toString() + "' occurs");
                                exMsgBldr.append(" while processing FlowFile '" + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{f.get().invokeMethod("getAttribute", new Object[]{"filename"})}) + "'");

                                exMsgBldr.append(" in '" + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{f.get().invokeMethod("getAttribute", new Object[]{"business.process.name"})}) + "' scenario");
                                exMsgBldr.append(" at '" + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{it.id}) + "' target");
                                exMsgBldr.append(" at " + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{f.get().invokeMethod("getAttribute", new Object[]{"xform.path"})}) + " path");
                                exMsgBldr.append(" at " + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{f.get().invokeMethod("getAttribute", new Object[]{"xform.stage"})}) + " stage");

                                Groovy.this.getBinding().getProperty("log").invokeMethod("log", new Object[]{LogLevel.ERROR, exMsgBldr.toString()});
                                Groovy.this.getBinding().getProperty("log").invokeMethod("log", new Object[]{LogLevel.ERROR, Groovy.this.getBinding().getProperty("traceOut")});

                                Groovy.this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{f.get(), "error.msg", ex1.toString()});

                                //failFlowFiles << flowFileCopy

                                Groovy.this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{f.get(), Groovy.this.getBinding().getProperty("REL_FAILURE")});
                            }

                        }


                        //log.log(org.apache.nifi.logging.LogLevel.ERROR, traceOut1)
            /*targets.clear()
            if (iflow.trace) {
                switch (iflow.trace) {
            case 'error':
                        log.log(org.apache.nifi.logging.LogLevel.ERROR, traceOut)
                        break
            case 'info':
                        log.log(org.apache.nifi.logging.LogLevel.INFO, traceOut)
                        break
                }
            }
            iflow.clear()
            traceCount = 0*/
                    }

                });
            }

            //if (iflow.trace == "rollback"){
            //    session.rollback()
            //    } else {
            //session.transfer(flowFiles, REL_SUCCESS)
            //session.transfer(failFlowFiles, REL_FAILURE)
            //}
            //log.log(org.apache.nifi.logging.LogLevel.INFO, traceOut)
        } catch (Exception ex) {
            trace("!!!!!!!Exception: " + ex.toString());
            this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, "error.msg", ex.toString()});
            this.getBinding().getProperty("log").invokeMethod("log", new Object[]{LogLevel.ERROR, this.getBinding().getProperty("traceOut")});
            return this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{flowFile, this.getBinding().getProperty("REL_FAILURE")});
        } finally {
            this.getBinding().getProperty("log").invokeMethod("log", new Object[]{LogLevel.INFO, this.getBinding().getProperty("traceOut1")});
        }


/**
 * getServiceController
 *
 * @param
 */


/**
 * transformFormat
 *
 * @param
 */
//@CompileStatic
/*def transformFormat(FlowFile localFlowFile, ByteArrayInputStream inputStream, String readerName,
String writerName, org.apache.nifi.processor.ProcessSession session) {
    //log.info "\r\n#######"
    trace "start transform format with reader ${readerName} to writer ${writerName}"
    org.apache.nifi.serialization.RecordReaderFactory readerFactory    = getServiceController(readerName)
    as org.apache.nifi.serialization.RecordReaderFactory
    org.apache.nifi.serialization.RecordSetWriterFactory writerFactory = getServiceController(writerName)
    as org.apache.nifi.serialization.RecordSetWriterFactory

    def originalAttributes = localFlowFile.getAttributes()
    FlowFile original = localFlowFile

    ByteArrayOutputStream result = new ByteArrayOutputStream()

    def reader = readerFactory.createRecordReader(originalAttributes, inputStream, original.getSize(), log)
    Record firstRecord = reader.nextRecord()

    org.apache.nifi.serialization.record.RecordSchema writeSchema = writerFactory.getSchema(originalAttributes,
     firstRecord.getSchema())
    org.apache.nifi.serialization.RecordSetWriter writer = writerFactory.createWriter(log, writeSchema,
     result, originalAttributes)
    writer.beginRecordSet()
    writer.write(firstRecord)
    Record record
    long count = 1L
    while ((record = reader.nextRecord()) != null) {
        writer.write(record); ++count;
    }
    //org.apache.nifi.serialization.WriteResult writeResult = writer.finishRecordSet()
    writer.flush()
    ByteArrayInputStream bis = new ByteArrayInputStream(result.toByteArray())
    // trace "transform result:\r\n ${stream2string(bis)}"
    return bis
}*/


//Perform xslt on the content represented by an input stream and write the result to provided output stream


//Returns processed FlowFile or ArrayList of processed FlowFiles


        return null;

    }

    /**
     * getServiceController
     *
     * @param
     */
    public Object getServiceController(final String name) {
        trace("get service controller: " + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{name}));
        final Object lookup = this.getBinding().getProperty("context").controllerServiceLookup;
        Object ServiceId = lookup.invokeMethod("getControllerServiceIdentifiers", new Object[]{org.apache.nifi.controller.ControllerService}).invokeMethod("find", new Object[]{new Closure<Boolean>(this, this) {
            public Boolean doCall(Object cs) {
                return lookup.invokeMethod("getControllerServiceName", new Object[]{cs}).equals(name);
            }

        }});
        return lookup.invokeMethod("getControllerService", new Object[]{ServiceId});
    }

    /**
     * transformFormat
     *
     * @param
     */
    public List evaluateXPath(InputStream inputStream, String xpathQuery) {
        Object records = this.getBinding().getProperty("builder").invokeMethod("parse", new Object[]{inputStream}).documentElement;
        //inputStream.reset()
        //def nodes = xpath.evaluate( xpathQuery, records, XPathConstants.NODESET )
        Object nodes = this.getBinding().getProperty("xpath").invokeMethod("evaluate", new Object[]{xpathQuery, records, XPathConstants.NODESET});
        return DefaultGroovyMethods.collect(nodes, new Closure(this, this) {
            public Object doCall(Object node) {
                return node.textContent;
            }

        });
//String res = xpath.evaluate(xpathQuery, records)
//nodes.collect { node -> node.textContent }
    }

    public Object evaluateXPathValue(InputStream inputStream, String xpathQuery) {
        Object records = this.getBinding().getProperty("builder").invokeMethod("parse", new Object[]{inputStream}).documentElement;
        //inputStream.reset()
        //def nodes = xpath.evaluate( xpathQuery, records, XPathConstants.NODESET )
        Object res = this.getBinding().getProperty("xpath").invokeMethod("evaluate", new Object[]{xpathQuery, records});
        return res;
//String res = xpath.evaluate(xpathQuery, records)
//nodes.collect { node -> node.textContent }
    }

    public String stream2string(InputStream inputStream) {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        Byte[] buffer = new Byte[1024];
        for (int length; (length = inputStream.read(buffer)) != -1; ) {
            result.write(buffer, 0, length);
        }

        // StandardCharsets.UTF_8.name() > JDK 7
        inputStream.reset();
        return result.toString("UTF-8");
    }

    public Byte[] stream2byteArray(InputStream inputStream) {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        Byte[] buffer = new Byte[1024];
        for (int length; (length = inputStream.read(buffer)) != -1; ) {
            result.write(buffer, 0, length);
        }

        // StandardCharsets.UTF_8.name() > JDK 7
        return result.toByteArray();
//return new ByteArrayInputStream(result.toByteArray())
    }

    public String xml2Json(String xml) throws Exception {
        Object parsed = new XmlParser().invokeMethod("parseText", new Object[]{xml});

        final Reference<Object> handle;
        handle.set(new Closure<Serializable>(this, this) {
            public Serializable doCall(Object node) {
                if (node instanceof String) {
                    return node;
                } else {
                    LinkedHashMap<Object, List<Object>> map = new LinkedHashMap<Object, List<Object>>(1);
                    map.put(node.invokeMethod("name", new Object[0]), DefaultGroovyMethods.collect(node, (Closure<Object>) handle.get()));
                    return map;
                }

            }

        });

        LinkedHashMap<Object, List<LinkedHashMap<Object, List<Object>>>> map = new LinkedHashMap<Object, List<LinkedHashMap<Object, List<Object>>>>(1);
        map.put(parsed.invokeMethod("name", new Object[0]), DefaultGroovyMethods.collect(parsed, new Closure<LinkedHashMap<Object, List<Object>>>(this, this) {
            public LinkedHashMap<Object, List<Object>> doCall(Object node) {
                LinkedHashMap<Object, List<Object>> map1 = new LinkedHashMap<Object, List<Object>>(1);
                map1.put(node.invokeMethod("name", new Object[0]), DefaultGroovyMethods.collect(node, (Closure<Object>) handle.get()));
                return map1;
            }

        }));
        LinkedHashMap<Object, List<LinkedHashMap<Object, List<Object>>>> jsonObject = map;

        return ((String) (new groovy.json.JsonBuilder(jsonObject).invokeMethod("toString", new Object[0])));
    }

    public FlowFile convertFlowFile(FlowFile flowFile) throws Exception {
        flowFile = ((FlowFile) (this.getBinding().getProperty("session").invokeMethod("write", new Object[]{flowFile, new StreamCallback() {
            @Override
            public void process(InputStream is, OutputStream os) throws IOException {
                Byte[] content = stream2byteArray(is);
                trace("Len " + content.length);
                String json = xml2Json(new String(content));
                if (StringGroovyMethods.asBoolean(json)) {
                    os.write(json.getBytes());
                    os.flush();
                } else {
                    throw new Exception("Failed xml convertation!");
                }

            }

        }})));
        return flowFile;
    }

    public void applyXslt(InputStream flowFileContent, OutputStream os, final String transformName) throws IOException, IllegalArgumentException {
        if (!StringGroovyMethods.asBoolean(transformName)) {
            throw new IOException("XSLT with the name " + transformName + " not found");
        }

        trace("apply xslt transform: " + transformName);

        Object xslt = this.getBinding().getProperty("xsltCacheMap").invokeMethod("get", new Object[]{transformName, new Serializer<String>() {
            @Override
            public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
                out.write(value.getBytes(StandardCharsets.UTF_8));
            }

        }, new Deserializer<String>() {
            @Override
            public String deserialize(final Byte[] value) throws DeserializationException, IOException {
                if (value == null) {
                    return null;
                }

                return new String(value, StandardCharsets.UTF_8);
            }

        }});
        //def xslt = xsltCacheMap.lookup(creds);

        if (!DefaultGroovyMethods.asBoolean(xslt)) {
            trace("transfom not found in cache: " + transformName);
            this.getBinding().getProperty("log").invokeMethod("error", new Object[]{"transfom not found in cache:" + transformName});
            throw new IOException("XSLT with the name " + transformName + " not found");
        }

        Transformer transformer;
        if (transformName.equals("RemoveEnvelope.xsl")) {
            if (this.getBinding().getProperty("xslRemoveEnv").asBoolean()) {
                transformer = ((Transformer) (this.getBinding().getProperty("xslRemoveEnv")));
            } else {
                transformer = TransformerFactory.newInstance().newTransformer(new StreamSource(new StringReader((String) xslt)));
                setProperty("xslRemoveEnv", transformer);
            }

        } else {
            transformer = TransformerFactory.newInstance().newTransformer(new StreamSource(new StringReader((String) xslt)));
        }

        //trace('Before actual transformation')

        //ByteArrayOutputStream result = new ByteArrayOutputStream()
        //ByteArrayOutputStream baos = new ByteArrayOutputStream()
        //trace "start transform for:\r\n${stream2string(flowFileContent)}"
        //Writer writer = new OutputStreamWriter(baos)
        Writer writer = new OutputStreamWriter(os);

        //trace('Transformation ' + String.valueOf(flowFileContent.available()))
        StreamResult strmres = new StreamResult(writer);

        // transformer.transform(new StreamSource(flowFileContent), new StreamResult(result))
        transformer.transform(new StreamSource(flowFileContent), strmres);
    /*trace('After transformation')
    byte[] res = baos.toByteArray()
    trace(new String(res))
    ByteArrayInputStream bis = new ByteArrayInputStream(res)
    //trace "transform result:\r\n${stream2string(bis)}"
    baos.close()
    return bis*/
//return 1
    }

    public String getResponse(String protocol, String code) {
        if (protocol.equals("XI")) {
            if (StringGroovyMethods.isCase("200", code)) {
                return "XI_OK";
            }
        } else {
            return code;
        }

    }

    public Map<String, String> parseParams(String url) throws Exception {
        Map<String, String> params = new HashMap<String, String>();
        String[] keyValuePairs = url.split("&");

        for (String pair : keyValuePairs) {
            String[] keyValuePair = pair.split("=");
            if (keyValuePair.length > 0) params.put(keyValuePair[0].trim(), keyValuePair[1].trim());
        }

        return params;
    }

    public void transferResult(Object result, boolean sync, List urlList, Object config) throws Exception {
        final int urlListSize = urlList.size();
        trace("Medved");

        if (result == null) trace("A result to null");

        if (result instanceof FlowFile) {
            trace("Single");
            final Reference<FlowFile> file = new Reference<FlowFile>((FlowFile) result);
            file.set(postprocessXform(file.get(), sync, config));
            trace("After postprocess");
            DefaultGroovyMethods.eachWithIndex(urlList, new Closure(this, this) {
                public Object doCall(Object j, Object index) {
                    if (DefaultGroovyMethods.and(index < urlListSize - 1, urlListSize > 1)) {
                        FlowFile f = (FlowFile) Groovy.this.getBinding().getProperty("session").clone();
                        f.target_url = String.valueOf(j);
                        return Groovy.this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{f, Groovy.this.getBinding().getProperty("REL_SUCCESS")});
                    } else {
                        if (file.get() == null) trace("Why null?");
                        file.get().target_url = String.valueOf(j);
                        //FlowFile ff = file as FlowFile
                        return Groovy.this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{file.get(), Groovy.this.getBinding().getProperty("REL_SUCCESS")});
                    }

                }

            });
        } else if (result instanceof ArrayList) {
            trace("array");
            ArrayList list = DefaultGroovyMethods.asType((Collection) result, ArrayList.class);
            trace(String.valueOf(list.size()));
            for (FlowFile f : list) {
                if (f == null) continue;
                final FlowFile f1 = postprocessXform(f, sync, config);

                DefaultGroovyMethods.eachWithIndex(urlList, new Closure(this, this) {
                    public Object doCall(Object j, Object index) {
                        if (DefaultGroovyMethods.and(index < urlListSize - 1, urlListSize > 1)) {
                            FlowFile fc = (FlowFile) Groovy.this.getBinding().getProperty("session").clone();
                            fc.target_url = String.valueOf(j);
                            return Groovy.this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{fc, Groovy.this.getBinding().getProperty("REL_SUCCESS")});
                        } else {
                            f.target_url = String.valueOf(j);
                            return Groovy.this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{f1, Groovy.this.getBinding().getProperty("REL_SUCCESS")});
                        }

                    }

                });
            }

        }

    }

    public Object processXform(FlowFile flowFile, ArrayList xforms, String targetId) throws Exception {
        boolean isFlowFileSuppressed = false;
        int prevStageIndx = -1;

        this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, "target.id", targetId});

        //If a flow file processed previosly, start right after the previous stage
        if (flowFile.invokeMethod("getAttribute", new Object[]{"xform.stage"}).asBoolean()) {
            prevStageIndx = Integer.parseInt(flowFile.invokeMethod("getAttribute", new Object[]{"xform.stage"}));
        } else {
            this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, "xform.stage", "0"});
        }


    /*trace "start processing target system: ${it.id}"
    trace "${it}"
    if (targetsRestrictList && targetsRestrictList.size() > 0) {
        if (!(it.id && targetsRestrictList.contains(it.id))) {
            trace "system ${it.id} not in access list, skip it"
            return
        }
    }*/
        boolean isPropagated = false;

        //Processing transformation for a target system
        int currStageIndx = -1;
        trace1("ID " + targetId);
        trace1("prev stage " + prevStageIndx);

        trace1(" " + xforms);
        this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, "xform.stage", "0"});

        this.getBinding().getProperty("session").invokeMethod("getProvenanceReporter", new Object[0]).invokeMethod("modifyContent", new Object[]{flowFile, "wsrhsrh"});

        for (String xform : xforms) {
            //Stop processing flow file if it is suppressed at routing stage
            if (isFlowFileSuppressed) return null;
            currStageIndx++;
            flowFile.xform.stage = currStageIndx;

            //Start process right after the previous stage
            if (currStageIndx > prevStageIndx) {
                trace1("Stage " + String.valueOf(currStageIndx));
                String[] nameParamsPair = xform.split("://");
                Map<String, String> params = null;
                if (nameParamsPair.length > 1) {
                    //Params must follow after ? symbol
                    params = parseParams(nameParamsPair[1].substring(1));
                }

                for (Map.Entry<String, String> paramEntry : params.entrySet()) {
                    trace("Key " + paramEntry.getKey() + " val " + paramEntry.getValue());
                }

                String name = nameParamsPair.length > 1 ? nameParamsPair[0] : xform;
                trace("processing " + String.valueOf(currStageIndx) + " stage");

                if (StringGroovyMethods.isCase(("SyncResponse"), name)) {
                    syncResponse(flowFile);
                } else if (StringGroovyMethods.isCase(("RouteOnContent"), name)) {
                    String param = params.get("MatchRequirement");
                    if (!StringGroovyMethods.asBoolean(param)) throw new IllegalArgumentException(name + " " + param);
                    flowFile.content.match.strategy = param;
                    param = params.get("RouteCondition");
                    if (!StringGroovyMethods.asBoolean(param)) throw new IllegalArgumentException(name + " " + param);
                    PropertyValue propValue = this.getBinding().getProperty("context").invokeMethod("newPropertyValue", new Object[]{param});
                    String s = DefaultGroovyMethods.invokeMethod(propValue, "evaluateAttributeExpressions", new Object[]{flowFile}).getValue();
                    flowFile.route.on.content.condition = s;
                    param = params.get("Result");
                    propValue = ((PropertyValue) (this.getBinding().getProperty("context").invokeMethod("newPropertyValue", new Object[]{param})));
                    s = DefaultGroovyMethods.invokeMethod(propValue, "evaluateAttributeExpressions", new Object[]{flowFile}).getValue();
                    flowFile.route.on.content.result = s;
                    flowFile.xform.group = "RouteOnContent";
                    isPropagated = true;
                } else if (StringGroovyMethods.isCase(("UpdateAttribute"), name)) {
                    for (Map.Entry<String, String> entry : params.entrySet()) {
                        PropertyValue propValue = this.getBinding().getProperty("context").invokeMethod("newPropertyValue", new Object[]{entry.getValue()});
                        String attrValue = DefaultGroovyMethods.invokeMethod(propValue, "evaluateAttributeExpressions", new Object[]{flowFile}).getValue();
                        flowFile = ((FlowFile) (this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, entry.getKey(), attrValue})));
                    }

                } else if (StringGroovyMethods.isCase(("RouteOnAttribute"), name)) {
                    String param = params.get("RoutingStrategy");
                    if (!StringGroovyMethods.asBoolean(param)) throw new IllegalArgumentException(name + " " + param);
                    param = params.get("Condition");
                    if (!StringGroovyMethods.asBoolean(param)) throw new IllegalArgumentException(name + " " + param);
                    PropertyValue propValue = this.getBinding().getProperty("context").invokeMethod("newPropertyValue", new Object[]{param});
                    String res = DefaultGroovyMethods.invokeMethod(propValue, "evaluateAttributeExpressions", new Object[]{flowFile}).getValue();
                    if (res.equals("false")) {
                        isFlowFileSuppressed = true;
                        //throw new Exception('Result ' + res + ' does not match condition')
                    }

                } else if (StringGroovyMethods.isCase(("ReplaceText"), name)) {
                    String replacementStrategy = params.get("ReplacementStrategy");
                    if (!StringGroovyMethods.asBoolean(replacementStrategy))
                        throw new IllegalArgumentException(name + " " + this.getBinding().getProperty("param"));
                    flowFile.replace.text.mode = replacementStrategy;
                    String searchValue = params.get("SearchValue");
                    if (!StringGroovyMethods.asBoolean(searchValue))
                        throw new IllegalArgumentException(name + " " + this.getBinding().getProperty("param"));
                    PropertyValue propValue = this.getBinding().getProperty("context").invokeMethod("newPropertyValue", new Object[]{searchValue});
                    searchValue = DefaultGroovyMethods.invokeMethod(propValue, "evaluateAttributeExpressions", new Object[]{flowFile}).getValue();
                    flowFile.replace.text.search.value = searchValue;
                    String replacementValue = params.get("ReplacementValue");
                    if (!StringGroovyMethods.asBoolean(replacementValue))
                        throw new IllegalArgumentException(name + " " + this.getBinding().getProperty("param"));
                    propValue = ((PropertyValue) (this.getBinding().getProperty("context").invokeMethod("newPropertyValue", new Object[]{replacementValue})));
                    replacementValue = DefaultGroovyMethods.invokeMethod(propValue, "evaluateAttributeExpressions", new Object[]{flowFile}).getValue();
                    flowFile.replace.text.replacement.value = replacementValue;
                    final int fileSize = (int) ((FlowFile) flowFile).getSize();
                    flowFile = replaceText(flowFile, replacementStrategy, searchValue, replacementValue, "EntireText", StandardCharsets.UTF_8, fileSize);
                } else if (StringGroovyMethods.isCase(("EvaluateXQuery"), name)) {
                    String param = params.get("Destination");
                    if (!param.equals("flowfile-attribute")) throw new IllegalArgumentException(name + " " + param);
                    params.remove("Destination");
                    for (Map.Entry<String, String> paramEntry : params.entrySet()) {
                        trace("Processing " + paramEntry.getKey() + " ");

                        if (paramEntry.getValue().indexOf("count(") > -1) {
                            trace("+count");
                            final StringBuilder sb = new StringBuilder();
                            this.getBinding().getProperty("session").invokeMethod("read", new Object[]{flowFile, new InputStreamCallback() {
                                @Override
                                public void process(InputStream is) throws IOException {
                                    Object r = evaluateXPathValue(is, paramEntry.getValue().replace("\\\\", "\\"));
                                    sb.append(r);
                                }

                            }});
                            this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, paramEntry.getKey(), sb.toString()});
                        } else {
                            //final ByteArrayOutputStream baos = new ByteArrayOutputStream()
                            final List<String> list = new ArrayList<String>();

                            this.getBinding().getProperty("session").invokeMethod("read", new Object[]{flowFile, new InputStreamCallback() {
                                @Override
                                public void process(InputStream is) throws IOException {
                                    List nodes = evaluateXPath(is, paramEntry.getValue().replace("\\\\", "\\"));

                                    for (Object node : nodes) {
                                        list.add(node);
                                    }


                                    /*for (int j = 0; j < nodes.getLength(); j++) {
                                        def node = nodes.item(j)
                                        list.add(node.toString())
                                    }*/
                                    //baos.write((evaluateXPath(is,
                                    //    paramEntry.getValue().replace('\\\\', '\\'))).getBytes())
                                }

                            }});

                            //res = baos.toString()
                            trace1("+res");
                            if (list.size() == 1) {
                                this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, paramEntry.getKey(), list.get(0)});
                                trace1("EvalXq res " + paramEntry.getKey() + " ".plus(list.get(0)));
                            } else {
                                int sfx = 1;
                                for (String s : list) {
                                    final String attrName = paramEntry.getKey() + "." + String.valueOf(sfx);
                                    trace1("EvalXq res " + attrName + " ".plus(s));
                                    this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, attrName, s});
                                    sfx++;
                                }

                            }

                        }


                        //String r = Arrays.toString(res)
                    }

                } else if (StringGroovyMethods.isCase(("ApplyXslt"), name)) {
                    final String param = params.get("Name");
                    if (!StringGroovyMethods.asBoolean(param)) throw new IllegalArgumentException(name + " " + param);
                    flowFile = ((FlowFile) (this.getBinding().getProperty("session").invokeMethod("write", new Object[]{flowFile, new StreamCallback() {
                        @Override
                        public void process(InputStream is, OutputStream os) throws IOException {
                            applyXslt(is, os, param);
                            os.flush();
                        }

                    }})));
                } else if (StringGroovyMethods.isCase(("DuplicateFlowFile"), name)) {
                    String param = params.get("Number");
                    if (!StringGroovyMethods.asBoolean(param)) throw new IllegalArgumentException(name + " " + param);
                    PropertyValue propValue = this.getBinding().getProperty("context").invokeMethod("newPropertyValue", new Object[]{param});
                    param = DefaultGroovyMethods.invokeMethod(propValue, "evaluateAttributeExpressions", new Object[]{flowFile}).getValue();
                    int numOfCopies = Integer.parseInt(param);
                    ArrayList<FlowFile> res = (ArrayList<FlowFile>) new ArrayList<E>();
                    flowFile.copy.index = 0;
                    if (currStageIndx == xforms.size() - 1) {
                        flowFile = ((FlowFile) (this.getBinding().getProperty("session").invokeMethod("removeAttribute", new Object[]{this.getBinding().getProperty("f"), "xform.group"})));
                    }

                    String ffid = flowFile.invokeMethod("getAttribute", new Object[]{"uuid"});
                    for (int i = 0; i < numOfCopies; i++) {
                        FlowFile f = (FlowFile) this.getBinding().getProperty("session").clone();
                        f.copy.index = String.valueOf(i + 1);
                        graylogNotifyStart(f, ffid);
                        FlowFile ff = null;
                        if (currStageIndx < xforms.size() - 1) {
                            ff = ((FlowFile) (processXform(f, xforms, targetId)));
                        }

                        if (ff == null) {
                            this.getBinding().getProperty("session").invokeMethod("remove", new Object[]{f});
                        } else {
                            res.add(ff);
                        }

                    }

                    if (currStageIndx < xforms.size() - 1) {
                        setProperty("ff", processXform(flowFile, xforms, targetId));
                        if (this.getBinding().getProperty("ff") == null) {
                            this.getBinding().getProperty("session").invokeMethod("remove", new Object[]{flowFile});
                        } else {
                            res.add(this.getBinding().getProperty("ff"));
                        }

                    } else {
                        res.add(flowFile);
                    }

                    return res;
                } else {
                    for (Map.Entry<String, String> entry : params.entrySet()) {
                        flowFile = ((FlowFile) (this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, entry.getKey(), entry.getValue()})));
                    }

                    flowFile.xform.group = name;
                }
                graylogNotify(flowFile, name);
            }

            if (isPropagated) {
                break;
            }

        }

        trace("Stage is " + String.valueOf(currStageIndx) + " size " + String.valueOf(xforms.size()));
        if (currStageIndx == xforms.size() - 1) {
            if (isPropagated) {
                if (!DefaultGroovyMethods.asBoolean(flowFile.invokeMethod("getAttribute", new Object[]{"target.output"}).invokeMethod("equals", new Object[]{"JSON"}))) {
                    //Red pill for last xform to transfer flow file to the upload group
                    flowFile.xform.last = "true";
                }

            } else {
                if (flowFile.invokeMethod("getAttribute", new Object[]{"target.output"}).invokeMethod("equals", new Object[]{"JSON"}).asBoolean()) {
                    flowFile = convertFlowFile(flowFile);
                }

                //Move right to upload group
                flowFile = ((FlowFile) (this.getBinding().getProperty("session").invokeMethod("removeAttribute", new Object[]{flowFile, "xform.group"})));
            }

        }


        trace1("FF stage " + flowFile.invokeMethod("getAttribute", new Object[]{"xform.stage"}));
        return flowFile;
    /*else {
    trace 'Non list xform'
    fcCopy = applyXslt(fcCopy, it.transform)
    }*/

        //if (it.transform instanceof List)
        //it.transform.each { fcCopy = applyTransform(fcCopy, it) }
        //else fcCopy = applyTransform(fcCopy, it.transform)

    /* if (it.writer && iflow.reader) {
         flowFileCopy.'schema.name' = iflow.'reader.schema.name'
         trace "reader schema name found: ${flowFileCopy.'schema.name'}"

 //            newFlowFile."writer.schema.name" = iflow."writer.schema.name" != null ?  iflow."writer.schema.name" : iflow."reader.schema.name"
 //            trace "writer schema name found: ${newFlowFile."writer.schema.name"}"

         // add <root> tag for data
         ByteArrayOutputStream resultRooted = new ByteArrayOutputStream()
         byte[] buffer = new byte[1024]
         resultRooted.write('<root>'.getBytes(), 0, 6)
         for (int length; (length = fcCopy.read(buffer)) != -1;) {
             resultRooted.write(buffer, 0, length)
         }
         resultRooted.write('</root>'.getBytes(), 0, 7)
         fcCopy = new ByteArrayInputStream(resultRooted.toByteArray())
             //trace "added root tag:\r\n${stream2string(fcCopy)}"

    //fcCopy = transformFormat(flowFileCopy, fcCopy, iflow.reader, it.writer, session)
    }

     if (it.output == 'SOAP') {
         fcCopy = applyXslt(fcCopy, 'AddSoapEnvelope.xsl')
     }*/
    }

    public FlowFile postprocessXform(FlowFile flowFile, boolean syncStatus, final Object config) throws Exception {
        if (syncStatus) {
            trace("flowfile marked as SYNC, response flow name is: " + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{config.response}));
            flowFile.iflow.status.code = "";
            this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{flowFile, "business.process.name", config.response});
            //flowFileCopy.flow_name = it.response
        }
//else {
        //flowFileCopy.'iflow.status.code' = getResponse(iflow.input, '200')
        //}
        trace("Before potentional problem");
        if (flowFile == null) trace("A eto null");
        flowFile.iflow.input = config.input;
        //flowFile.'iflow.mapping' = config.mapping
        //if(it.id == "rdr") newFlowFile.'iflow.mapping' = "rdr/SKBP_Response"
        flowFile.iflow.sync = config.sync;
        flowFile.processGroupId = "2fde38c3-b6b5-1fee-0c7c-7c06e1792e1a";

        //log.error context.getProperty(it.id).getValue()
        trace("Prost");

        //trace "set target URL as system IP/DNS from parameter ${it.id}(value ${context.getProperty(it.id).getValue()}) and path ${j}"
        if (this.getBinding().getProperty("context").invokeMethod("getProperty", new Object[]{config.id}).invokeMethod("getValue", new Object[0]) == null) {
            this.getBinding().getProperty("log").invokeMethod("error", new Object[]{"Property for " + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{config.id}) + " not found, add it to processor parameters!"});
            //newFlowFile.'target_url' = "" + context.getProperty(it.id) + i
            flowFile.target_system = config.id;
        }


        return flowFile;
    }

    public int findTarget(ArrayList targets, String id) {
        int i = -1;
        for (Object target : targets) {
            i++;
            if (target.id.equals(id)) {
                return i;
            }

        }

        return -1;
    }

    public Object trace(String message) {
        return setProperty0(this, "traceOut", this.getBinding().getProperty("traceOut") + "\r\n+++++++ " + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{this.getBinding().getProperty("traceCount")++}) + " +++++++:" + message);
//    log.log(LogLevel.INFO, message)
    }

    public Object trace1(String message) {
        return setProperty0(this, "traceOut1", this.getBinding().getProperty("traceOut1") + "\r\n+++++++ " + DefaultGroovyMethods.invokeMethod(String.class, "valueOf", new Object[]{this.getBinding().getProperty("traceCount1")++}) + " +++++++:" + message);
//    log.log(LogLevel.INFO, message)
    }

    public void graylogNotify(FlowFile flowFile, String xformEntity) throws Exception {
        String sender = flowFile.invokeMethod("getAttribute", new Object[]{"http.query.param.senderService"});
        if (!StringGroovyMethods.asBoolean(sender)) {
            sender = "Не указан";
        }

        String processGroupId = "60484390-d08c-1fe2-b9a9-d47458b352ee";
        String processGroupName = "Transform";
        String hostName = InetAddress.getLocalHost().getHostName();
        String fileName = flowFile.invokeMethod("getAttribute", new Object[]{"filename"});
        String uuid = flowFile.invokeMethod("getAttribute", new Object[]{"uuid"});
        String pathVal = flowFile.invokeMethod("getAttribute", new Object[]{"path"});
        String requestUri = flowFile.invokeMethod("getAttribute", new Object[]{"http.request.uri"});
        if (requestUri.equals("/sap/xi")) {
            requestUri = ((String) (flowFile.invokeMethod("getAttribute", new Object[]{"sap.Interface.value"})));
        }


        String xformPath = flowFile.invokeMethod("getAttribute", new Object[]{"xform.path"});
        if (xformPath == null) {
            xformPath = "unknow";
        }


        String xformStage = flowFile.invokeMethod("getAttribute", new Object[]{"xform.stage"});
        if (xformStage == null) {
            xformStage = "unknow";
        }


        //Определение получателя
        String receiver = "Не определен";
        Object receiverLookup = this.getBinding().getProperty("receiverServiceId").invokeMethod("asControllerService", new Object[]{this.getBinding().getProperty("StringLookupService")});
        if (receiverLookup.asBoolean()) {
            LinkedHashMap<String, String> map = new LinkedHashMap<String, String>(1);
            map.put("key", requestUri);
            LinkedHashMap<String, String> coordinate = map;
            Object value = receiverLookup.invokeMethod("lookup", new Object[]{coordinate});
            if (value.invokeMethod("isPresent", new Object[0]).asBoolean()) {
                receiver = ((String) (value.invokeMethod("get", new Object[0])));
            }

        }


        if (receiver.equals("attribute")) {
            receiver = ((String) (flowFile.invokeMethod("getAttribute", new Object[]{"Receiver"})));
        }

        if (!StringGroovyMethods.asBoolean(receiver)) {
            receiver = "Не определен";
        }


        //Определение бизнес-процесса
        String businessProcessName = flowFile.invokeMethod("getAttribute", new Object[]{"business.process.name"});
    /*def serviceLookup = clientServiceId.asControllerService(StringLookupService)
    if (serviceLookup) {
        def coordinate = [key: requestUri]
        def value = serviceLookup.lookup(coordinate)
        if (value.isPresent()) {
            businessProcessName = value.get()
        }
    }*/

        String specUrl = "https://1desb-s-app01.gk.rosatom.local/nifi-docs/components/ru.greenatom.atombridge/af-specification-nar/4.0.0.0/ru.greenatom.af.Specifications/index.html#" + businessProcessName;

        //Формирование GELF-сообщения
        String shortMessage = "Сообщение в [" + processGroupName + "] c filename [" + fileName + "], бизнес-процесс [" + businessProcessName + "], отправитель [" + sender + "], получатель [" + receiver + "]";

        LinkedHashMap<String, String> map1 = new LinkedHashMap<String, String>(18);
        map1.put("_fileName", fileName);
        map1.put("path", pathVal);
        map1.put("short_message", shortMessage);
        map1.put("host", hostName);
        map1.put("facility", processGroupName);
        map1.put("_groupId", processGroupId);
        map1.put("level", "INFO");
        map1.put("_groupName", processGroupName);
        map1.put("_messageUuid", uuid);
        map1.put("_requestUrl", requestUri);
        map1.put("_sender", sender);
        map1.put("_receiver", receiver);
        map1.put("_entryType", "processing");
        map1.put("_businessProcess", businessProcessName);
        map1.put("specification", specUrl);
        map1.put("transformationEntity", xformEntity);
        map1.put("transformationPath", xformPath);
        map1.put("transformationStage", xformStage);
        LinkedHashMap<String, String> map = map1;
        Object json = this.getBinding().getProperty("groovy").json.JsonOutput.invokeMethod("toJson", new Object[]{map});
        //Отправка GELF-сообщения
        URLConnection post = new URL("http://1tesb-s-grl01.gk.rosatom.local:12001/gelf").openConnection();
        DefaultGroovyMethods.invokeMethod(post, "setRequestMethod", new Object[]{"POST"});
        post.setDoOutput(true);
        post.setRequestProperty("Content-Type", "application/json");
        DefaultGroovyMethods.invokeMethod(post.getOutputStream(), "write", new Object[]{json.invokeMethod("getBytes", new Object[]{"UTF-8"})});
        Object postRC = DefaultGroovyMethods.invokeMethod(post, "getResponseCode", new Object[0]);
        if (postRC < 200 && postRC > 300) {
            throw new Exception("Ошибка отправки, код " + postRC);
        }

    }

    public void graylogNotifyStart(FlowFile flowFile, String derivationId) throws Exception {
        String sender = flowFile.invokeMethod("getAttribute", new Object[]{"http.query.param.senderService"});
        if (!StringGroovyMethods.asBoolean(sender)) {
            sender = "Не указан";
        }

        String processGroupId = "60484390-d08c-1fe2-b9a9-d47458b352ee";
        String processGroupName = "Transform";
        String hostName = InetAddress.getLocalHost().getHostName();
        String fileName = flowFile.invokeMethod("getAttribute", new Object[]{"filename"});
        String uuid = flowFile.invokeMethod("getAttribute", new Object[]{"uuid"});
        String pathVal = flowFile.invokeMethod("getAttribute", new Object[]{"path"});
        String requestUri = flowFile.invokeMethod("getAttribute", new Object[]{"http.request.uri"});
        if (requestUri.equals("/sap/xi")) {
            requestUri = ((String) (flowFile.invokeMethod("getAttribute", new Object[]{"sap.Interface.value"})));
        }


        //Определение получателя
        String receiver = "Не определен";
        Object receiverLookup = this.getBinding().getProperty("receiverServiceId").invokeMethod("asControllerService", new Object[]{this.getBinding().getProperty("StringLookupService")});
        if (receiverLookup.asBoolean()) {
            LinkedHashMap<String, String> map = new LinkedHashMap<String, String>(1);
            map.put("key", requestUri);
            LinkedHashMap<String, String> coordinate = map;
            Object value = receiverLookup.invokeMethod("lookup", new Object[]{coordinate});
            if (value.invokeMethod("isPresent", new Object[0]).asBoolean()) {
                receiver = ((String) (value.invokeMethod("get", new Object[0])));
            }

        }


        if (receiver.equals("attribute")) {
            receiver = ((String) (flowFile.invokeMethod("getAttribute", new Object[]{"Receiver"})));
        }

        if (!StringGroovyMethods.asBoolean(receiver)) {
            receiver = "Не определен";
        }


        //Определение бизнес-процесса
        String businessProcessName = flowFile.invokeMethod("getAttribute", new Object[]{"business.process.name"});
    /*def serviceLookup = clientServiceId.asControllerService(StringLookupService)
    if (serviceLookup) {
        def coordinate = [key: requestUri]
        def value = serviceLookup.lookup(coordinate)
        if (value.isPresent()) {
            businessProcessName = value.get()
        }
    }*/

        String specUrl = "https://1desb-s-app01.gk.rosatom.local/nifi-docs/components/ru.greenatom.atombridge/af-specification-nar/4.0.0.0/ru.greenatom.af.Specifications/index.html#" + businessProcessName;

        //Формирование GELF-сообщения
        String shortMessage = "Сообщение в [" + processGroupName + "] c filename [" + fileName + "], бизнес-процесс [" + businessProcessName + "], отправитель [" + sender + "], получатель [" + receiver + "]";

        LinkedHashMap<String, Serializable> map1 = new LinkedHashMap<String, Serializable>(18);
        map1.put("_fileName", fileName);
        map1.put("path", pathVal);
        map1.put("short_message", shortMessage);
        map1.put("host", hostName);
        map1.put("facility", processGroupName);
        map1.put("_groupId", processGroupId);
        map1.put("level", "INFO");
        map1.put("_groupName", processGroupName);
        map1.put("_messageUuid", uuid);
        map1.put("_requestUrl", requestUri);
        map1.put("_sender", sender);
        map1.put("_receiver", receiver);
        map1.put("_entryType", "start");
        map1.put("_LogStart", 1);
        map1.put("_LogSuccess", 0);
        map1.put("_businessProcess", businessProcessName);
        map1.put("specification", specUrl);
        map1.put("derivation", derivationId);
        LinkedHashMap<String, Serializable> map = map1;
        Object json = this.getBinding().getProperty("groovy").json.JsonOutput.invokeMethod("toJson", new Object[]{map});
        //Отправка GELF-сообщения
        URLConnection post = new URL("http://1tesb-s-grl01.gk.rosatom.local:12001/gelf").openConnection();
        DefaultGroovyMethods.invokeMethod(post, "setRequestMethod", new Object[]{"POST"});
        post.setDoOutput(true);
        post.setRequestProperty("Content-Type", "application/json");
        DefaultGroovyMethods.invokeMethod(post.getOutputStream(), "write", new Object[]{json.invokeMethod("getBytes", new Object[]{"UTF-8"})});
        Object postRC = DefaultGroovyMethods.invokeMethod(post, "getResponseCode", new Object[0]);
        if (postRC < 200 && postRC > 300) {
            throw new Exception("Ошибка отправки, код " + postRC);
        }

    }

    public FlowFile replaceText(final FlowFile flowFile, final String type, final String searchValue, final String replacementValue, final String evaluateMode, final Charset charset, final int maxBufferSize) throws Exception {
        if (type.equals("RegexReplace")) {
            return regexReplaceText(flowFile, searchValue, replacementValue, evaluateMode, charset, maxBufferSize);
        } else {
            throw new Exception("Incorrect replace strategy");
        }

    }

    private static String normalizeReplacementString(String replacement) {
        String replacementFinal = replacement;
        if (Pattern.compile("(\\$\\D)").matcher(replacement).find()) {
            replacementFinal = Matcher.quoteReplacement(replacement);
        }

        return replacementFinal;
    }

    public FlowFile regexReplaceText(final FlowFile flowFile, final String searchValue, final String replacementValue, final String evaluateMode, final Charset charset, final int maxBufferSize) throws Exception {
        final int numCapturingGroups = Pattern.compile(searchValue).matcher("").groupCount();
        //final AttributeValueDecorator quotedAttributeDecorator = Pattern::quote

        final String searchRegex = searchValue;
        final Pattern searchPattern = Pattern.compile(searchRegex);
        final Map<String, String> additionalAttrs = new HashMap<String, String>(numCapturingGroups);

        FlowFile updatedFlowFile;
        if (evaluateMode.equalsIgnoreCase("EntireText")) {
            final int flowFileSize = (int) flowFile.invokeMethod("getSize", new Object[0]);
            final int bufferSize = Math.min(maxBufferSize, flowFileSize);
            final Byte[] buffer = new Byte[bufferSize];

            this.getBinding().getProperty("session").invokeMethod("read", new Object[]{flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream is) throws IOException {
                    StreamUtils.fillBuffer(is, buffer, false);

                }

            }});

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


                //String replacement = replacementValueProperty.evaluateAttributeExpressions(flowFile, additionalAttrs, escapeBackRefDecorator).getValue();
                //replacement = escapeLiteralBackReferences(replacement, numCapturingGroups);
                //String replacementFinal = normalizeReplacementString(replacement);
                String replacementFinal = Groovy.normalizeReplacementString(replacementValue);

                matcher.appendReplacement(sb, replacementFinal);
            }


            if (matches > 0) {
                matcher.appendTail(sb);

                final String updatedValue = sb.toString();
                updatedFlowFile = ((FlowFile) (this.getBinding().getProperty("session").invokeMethod("write", new Object[]{flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream os) throws IOException {
                        os.write(updatedValue.getBytes(charset));

                    }

                }})));
            } else {
                return flowFile;
            }

        } else throw new Exception("unsupported evaluation mode");
        return updatedFlowFile;
    }

    public void syncResponse(FlowFile flowFile) {
        FlowFile syncResponseFile = this.getBinding().getProperty("session").invokeMethod("create", new Object[]{flowFile});
        this.getBinding().getProperty("session").invokeMethod("putAttribute", new Object[]{syncResponseFile, "sync.response", "true"});
        this.getBinding().getProperty("session").invokeMethod("transfer", new Object[]{this.getBinding().getProperty("REL_SUCCESS"), syncResponseFile});
    }

    public Groovy(Binding binding) {
        super(binding);
    }

    public Groovy() {
        super();
    }

    private static <Value> Value setProperty0(Groovy propOwner, String s, Value o) {
        propOwner.setProperty(s, o);
        return o;
    }
}
