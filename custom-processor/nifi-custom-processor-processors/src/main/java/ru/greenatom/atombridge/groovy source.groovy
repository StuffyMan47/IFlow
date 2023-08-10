package ru.greenatom.atombridge

import groovy.transform.CompileStatic
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.ProcessSession
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.serialization.record.Record

import javax.xml.transform.stream.StreamResult
import javax.xml.transform.stream.StreamSource

import javax.xml.xpath.*
import javax.xml.parsers.DocumentBuilderFactory
import org.apache.nifi.distributed.cache.client.Deserializer
import org.apache.nifi.distributed.cache.client.Serializer
import org.apache.nifi.distributed.cache.client.exception.DeserializationException
import org.apache.nifi.distributed.cache.client.exception.SerializationException

import org.apache.nifi.provenance.ProvenanceReporter

import java.io.IOException
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import org.apache.nifi.components.PropertyValue
import java.util.Arrays
import org.apache.nifi.lookup.StringLookupService

import javax.xml.XMLConstants
import javax.xml.transform.Source
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.*
import org.xml.sax.SAXException

import java.util.regex.Matcher
import java.util.regex.Pattern
import java.nio.charset.Charset

import org.apache.nifi.stream.io.StreamUtils

session = (ProcessSession) session

FlowFile flowFile = session.get()
if (!flowFile) return

xslRemoveEnv = null


traceOut = ''
traceCount = 0
traceOut1 = ''
traceCount1 = 0

xpath = XPathFactory.newInstance().newXPath()
builder = DocumentBuilderFactory.newInstance().newDocumentBuilder()

String iflowMapCacheLookupClientName = IflowMapCacheLookupClient.value
String xsdMapCacheLookupClientName = XsdMapCacheLookupClient.value
String xsltMapCacheLookupClientName = XsltMapCacheLookupClient.value


try {
    iflowCacheMap = getServiceController(iflowMapCacheLookupClientName)
    xsdCacheMap = getServiceController(xsdMapCacheLookupClientName)
    xsltCacheMap = getServiceController(xsltMapCacheLookupClientName)


    //trace("step 1: get iFlow config from cache: ${flowFile.getAttribute('business.scenario')}")

    // Optional<String> ret = iflowCacheMap.lookup(["key": flowFile.flow_name])
    //Map<String, String> creds = new HashMap<String, String>()
    //creds.put("key", flowFile.flow_name)
    //Optional<String> ret = iflowCacheMap.get(flowFile.flow_name,
    String ret = iflowCacheMap.get(flowFile.getAttribute('business.process.name'),
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

                }})
    if (!ret) {
        trace('iFlow not found, return 501')
        log.error "iFlow named:${flowFile.getAttribute('business.process.name')} not found!"
        flowFile.'iflow.error' = "iFlow named:${flowFile.getAttribute('business.process.name')} not found!"
        flowFile.'iflow.status.code' = getResponse('', '501')
        session.transfer(flowFile, REL_FAILURE)
        return
    } else {
        //trace("readed iFlow config:\r\n${ret.get()}")
        trace('readed iFlow config')
    }
    //    def iflow = flowFile.flow_name.endWIth(".json") ? new groovy.json.JsonSlurper().parseText(ret.get()) : new groovy.yaml.YamlSlurper().parseText(ret.get())
    trace('start parsing json iFlow')
    def iflow = new groovy.json.JsonSlurper().parseText(ret)
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

    ArrayList targets = iflow.targets as ArrayList
    trace "full list of defined target systems: ${targets}"
    //int lastIndex = targets.size() - 1
    //   ByteArrayInputStream fcCopy
    //if(lastIndex > 0) {
    //      fcCopy = stream2byteArray(fc)
    //      fc.reset()
    //  }

    boolean sync = Boolean.parseBoolean(iflow.sync)

    int numOfTargets = targets.size()
    if (flowFile.getAttribute('xform.stage') != null & flowFile.getAttribute('target.id') != null & flowFile.getAttribute('xform.path') != null) {
        try {
            trace1 '+loopback ' + flowFile.getAttribute('xform.stage')
            //trace1 '+loopback ' + flowFile.getAttribute('xform.stage') + ' ' flowFile.getAttribute('target.id')
            String targetId = flowFile.getAttribute('target.id')
            int targetIndx = findTarget(targets, targetId)
            if (targetIndx < 0) throw new IllegalArgumentException('Target not found')
            def target = targets.get(targetIndx)

            ArrayList xforms = target.transformations as ArrayList

            int xformPath = Integer.parseInt(flowFile.getAttribute('xform.path'))

            if (xformPath > -1 & xformPath < xforms.size()) {
                if (target.output == 'JSON') flowFile.putAttribute('target.output', 'JSON')
                def xform = xforms.get(xformPath)
                def result = processXform(flowFile, xform, targetId)
                if (result == null) {
                    trace '-ff'
                    session.remove(flowFile)
                } else {
                    List urlList = target.targetPath instanceof List ? target.targetPath : [target.targetPath]
                    transferResult(result, sync, urlList, target)
                }
            } else {
                throw new Exception('Incorrect transformation path ' + xformPath)
            }
        } catch (Exception ex1) {
            trace "!!!!!!!Exception: ${ex1.toString()}"
            StringBuilder exMsgBldr = new StringBuilder()
            exMsgBldr.append("Exception '${ex1.toString()}' occurs")
            exMsgBldr.append(" while processing FlowFile '${flowFile.getAttribute('filename')}'")

            exMsgBldr.append(" in '${flowFile.getAttribute('business.process.name')}' scenario")
            exMsgBldr.append(" at '${flowFile.getAttribute('target.id')}' target")
            exMsgBldr.append(" at '${flowFile.getAttribute('xform.path')}' path")
            exMsgBldr.append(" at ${flowFile.getAttribute('xform.stage')} stage")

            session.putAttribute(flowFile, 'error.msg', ex1.toString())

            log.log(org.apache.nifi.logging.LogLevel.ERROR, exMsgBldr.toString())
            log.log(org.apache.nifi.logging.LogLevel.ERROR, traceOut)

            session.putAttribute(flowFile, 'error.msg', ex1.toString())

            //failFlowFiles << flowFileCopy
            session.transfer(flowFile, REL_FAILURE)
            return
        }
    } else {
        //Validate against xsd schema
        flowFile.'xform.stage' = -1
        String schemaContent = null
        boolean isFailedSchemaExtraction = false
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

                        }})
            if (!schemaContent) {
                throw new IOException("Schema with name ${iflow.validate} not found")
            }
        } catch (Exception e) {
            String msg = 'Failed schema extraction! ' + e
            log.error msg
            flowFile.'error.msg' = msg
            session.transfer(flowFile, REL_FAILURE)
            isFailedSchemaExtraction = true
        }
        if (isFailedSchemaExtraction) return

        InputStream fis = null
        boolean isFailedValidation = false
        try {
            fis = session.read(flowFile)
            Source xmlFile = new StreamSource(fis)
            javax.xml.validation.SchemaFactory schemaFactory = SchemaFactory
                    .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
            javax.xml.validation.Schema schema = schemaFactory.newSchema(new StreamSource(new StringReader(schemaContent)))
            javax.xml.validation.Validator validator = schema.newValidator()
            validator.validate(xmlFile)
        } catch (Exception e) {
            String msg = 'Failed xml validation! ' + e
            log.error msg
            flowFile.'error.msg' = msg
            isFailedValidation = true
            session.transfer(flowFile, REL_FAILURE)
        } finally {
            fis.close()
        }
        if (isFailedValidation) return

        ProvenanceReporter reporter = session.getProvenanceReporter()

        targets.eachWithIndex { it, flowIndex ->
            ArrayList xforms = it.transformations as ArrayList
            //Make a copy of incoming flow file for each target system
            //Or use the incoming flowfile for last target
            //FlowFile file = flowIndex < numOfTargets - 1 & numOfTargets > 1 ? session.clone(flowFile) : flowFile
            FlowFile file = null
            if(flowIndex < numOfTargets - 1 & numOfTargets > 1){
                file = session.clone(flowFile)
                reporter.clone(flowFile, file)
            } else{
                file = flowFile
            }
            session.putAttribute(file, 'Receiver', it.id)
            int xformPath = -1

            if (it.syncValidation == 'true') {
                syncResponse(file)
            }

            if (it.output == 'JSON') {
                session.putAttribute(file, 'target.output', 'JSON')
            }

            FlowFile f = null

            for (ArrayList xform : xforms) {
                try {
                    xformPath++
                    session.putAttribute(file, 'xform.path', String.valueOf(xformPath))
                    f = xformPath < xforms.size() - 1 & xforms.size() > 1 ? session.clone(file) : file

                    def result = processXform(f, xform, it.id)
                    reporter.modifyContent(f)
                    if (result == null) {
                        session.remove(f)
                        return
                    } else {
                        List urlList = it.targetPath instanceof List ? it.targetPath : [it.targetPath]
                        transferResult(result, sync, urlList, it)
                    }
                } catch (Exception ex1) {
                    trace "!!!!!!!Exception: ${ex1.toString()}"
                    StringBuilder exMsgBldr = new StringBuilder()
                    exMsgBldr.append("Exception '${ex1.toString()}' occurs")
                    exMsgBldr.append(" while processing FlowFile '${f.getAttribute('filename')}'")

                    exMsgBldr.append(" in '${f.getAttribute('business.process.name')}' scenario")
                    exMsgBldr.append(" at '${it.id}' target")
                    exMsgBldr.append(" at ${f.getAttribute('xform.path')} path")
                    exMsgBldr.append(" at ${f.getAttribute('xform.stage')} stage")

                    log.log(org.apache.nifi.logging.LogLevel.ERROR, exMsgBldr.toString())
                    log.log(org.apache.nifi.logging.LogLevel.ERROR, traceOut)

                    session.putAttribute(f, 'error.msg', ex1.toString())

                    session.transfer(f, REL_FAILURE)
                }
            }
        }
    }

} catch (Exception ex) {
    trace "!!!!!!!Exception: ${ex.toString()}"
    session.putAttribute(flowFile, 'error.msg', ex.toString())
    log.log(org.apache.nifi.logging.LogLevel.ERROR, traceOut)
    session.transfer(flowFile, REL_FAILURE)
} finally {
    log.log(org.apache.nifi.logging.LogLevel.INFO, traceOut1)
}

/**
 * getServiceController
 *
 * @param
 */
def getServiceController(name) {
    trace "get service controller: ${name}"
    def lookup = context.controllerServiceLookup
    def ServiceId = lookup.getControllerServiceIdentifiers(org.apache.nifi.controller.ControllerService).find {
        cs -> lookup.getControllerServiceName(cs) == name
    }
    return lookup.getControllerService(ServiceId)
}

/**
 * transformFormat
 *
 * @param
 */


List evaluateXPath( InputStream inputStream, String xpathQuery ) {
    def records = builder.parse(inputStream).documentElement
    def nodes = xpath.evaluate(xpathQuery, records, XPathConstants.NODESET)
    return nodes.collect { node -> node.textContent }
//String res = xpath.evaluate(xpathQuery, records)
//nodes.collect { node -> node.textContent }
}

def evaluateXPathValue( InputStream inputStream, String xpathQuery ) {
    def records = builder.parse(inputStream).documentElement
    //inputStream.reset()
    //def nodes = xpath.evaluate( xpathQuery, records, XPathConstants.NODESET )
    def res = xpath.evaluate(xpathQuery, records)
    return res
//String res = xpath.evaluate(xpathQuery, records)
//nodes.collect { node -> node.textContent }
}

def stream2string(InputStream inputStream) {
    ByteArrayOutputStream result = new ByteArrayOutputStream()
    byte[] buffer = new byte[1024]
    for (int length; (length = inputStream.read(buffer)) != -1; ) {
        result.write(buffer, 0, length)
    }
    // StandardCharsets.UTF_8.name() > JDK 7
    inputStream.reset()
    return result.toString('UTF-8')
}

def stream2byteArray(InputStream inputStream) {
    ByteArrayOutputStream result = new ByteArrayOutputStream()
    byte[] buffer = new byte[1024]
    for (int length; (length = inputStream.read(buffer)) != -1; ) {
        result.write(buffer, 0, length)
    }
    // StandardCharsets.UTF_8.name() > JDK 7
    return result.toByteArray()
//return new ByteArrayInputStream(result.toByteArray())
}

String xml2Json(String xml) throws Exception{
    def parsed = new XmlParser().parseText( xml )

    def handle
    handle = { node ->
        if ( node instanceof String ) {
            node
        }
        else {
            [ (node.name()): node.collect( handle ) ]
        }
    }

    def jsonObject = [ (parsed.name()): parsed.collect { node ->
        [ (node.name()): node.collect( handle ) ]} ]

    return new groovy.json.JsonBuilder( jsonObject ).toString()
}

FlowFile convertFlowFile(FlowFile flowFile) throws Exception{
    flowFile = session.write(flowFile, new StreamCallback(){

        @Override
        void process(InputStream is, OutputStream os) throws IOException{
            byte[] content = stream2byteArray(is)
            trace 'Len ' + content.length
            String json = xml2Json(new String(content))
            if (json) {
                os.write(json.getBytes())
                os.flush()
            } else {
                throw new Exception('Failed xml convertation!')
            }
        }

    })
    return flowFile
}

//Perform xslt on the content represented by an input stream and write the result to provided output stream
void applyXslt(InputStream flowFileContent, OutputStream os, String transformName) throws IOException, IllegalArgumentException {
    if (!transformName) {
        throw new IOException("XSLT with the name ${transformName} not found")
    }
    trace("apply xslt transform: ${transformName}")

    def xslt = xsltCacheMap.get(transformName,
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
                }

            })
    //def xslt = xsltCacheMap.lookup(creds);

    if (!xslt) {
        trace("transfom not found in cache: ${transformName}")
        log.error 'transfom not found in cache:' + transformName
        throw new IOException("XSLT with the name ${transformName} not found")
    }
    javax.xml.transform.Transformer transformer
    if (transformName == 'RemoveEnvelope.xsl') {
        if (xslRemoveEnv) {
            transformer = xslRemoveEnv
        } else {
            transformer = javax.xml.transform.TransformerFactory.newInstance()
                    .newTransformer(new StreamSource(new StringReader(xslt)))
            xslRemoveEnv = transformer
        }
    } else {
        transformer = javax.xml.transform.TransformerFactory.newInstance()
                .newTransformer(new StreamSource(new StringReader(xslt)))
    }

    Writer writer = new OutputStreamWriter(os)

    StreamResult strmres =  new StreamResult(writer)

    // transformer.transform(new StreamSource(flowFileContent), new StreamResult(result))
    transformer.transform(new StreamSource(flowFileContent), strmres)
    /*trace('After transformation')
    byte[] res = baos.toByteArray()
    trace(new String(res))
    ByteArrayInputStream bis = new ByteArrayInputStream(res)
    //trace "transform result:\r\n${stream2string(bis)}"
    baos.close()
    return bis*/
//return 1
}

// делает Илья
def getResponse(String protocol, String code) {
    if (protocol == 'XI') {
        switch (code) {
            case '200':
                return 'XI_OK'
        }
    } else {
        return code
    }
}

Map<String, String> parseParams(String url) throws Exception {
    Map<String, String> params = new HashMap<>()
    String[] keyValuePairs = url.split('&')

    for (String pair : keyValuePairs) {
        String[] keyValuePair = pair.split('=')
        if (keyValuePair.length > 0) params.put(keyValuePair[0].trim(), keyValuePair[1].trim())
    }
    return params
}

void transferResult(result, boolean sync, List urlList, config) throws Exception{
    int urlListSize = urlList.size()
    trace 'Medved'

    if (result == null) trace 'A result to null'

    if (result instanceof FlowFile) {
        trace 'Single'
        FlowFile file = result as FlowFile
        file = postprocessXform(file, sync, config)
        trace 'After postprocess'
        urlList.eachWithIndex { j, index ->
            if (index < urlListSize - 1 & urlListSize > 1) {
                FlowFile f = session.clone(file)
                f.'target_url' = String.valueOf(j)
                session.transfer(f, REL_SUCCESS)
            } else {
                if (file == null) trace 'Why null?'
                file.'target_url' = String.valueOf(j)
                //FlowFile ff = file as FlowFile
                session.transfer(file, REL_SUCCESS)
            }
        }
    } else if (result instanceof ArrayList) {
        trace 'array'
        ArrayList list = result as ArrayList
        trace String.valueOf(list.size())
        for (FlowFile f : list) {
            if (f == null) continue
            FlowFile f1 = postprocessXform(f, sync, config)

            urlList.eachWithIndex { j, index ->
                if (index < urlListSize - 1 & urlListSize > 1) {
                    FlowFile fc = session.clone(f1)
                    fc.'target_url' = String.valueOf(j)
                    session.transfer(fc, REL_SUCCESS)
                } else {
                    f.'target_url' = String.valueOf(j)
                    session.transfer(f1, REL_SUCCESS)
                }
            }
        }
    }
}

//Returns processed FlowFile or ArrayList of processed FlowFiles
def processXform(FlowFile flowFile, ArrayList xforms, String targetId) throws Exception{
    boolean isFlowFileSuppressed = false
    int prevStageIndx = -1

    session.putAttribute(flowFile, 'target.id', targetId)

    //If a flow file processed previosly, start right after the previous stage
    if (flowFile.getAttribute('xform.stage')) {
        prevStageIndx = Integer.parseInt(flowFile.getAttribute('xform.stage'))
    } else {
        session.putAttribute(flowFile, 'xform.stage', '0')
    }

    /*trace "start processing target system: ${it.id}"
    trace "${it}"
    if (targetsRestrictList && targetsRestrictList.size() > 0) {
        if (!(it.id && targetsRestrictList.contains(it.id))) {
            trace "system ${it.id} not in access list, skip it"
            return
        }
    }*/
    boolean isPropagated = false

    //Processing transformation for a target system
    int currStageIndx = -1
    trace1 'ID ' + targetId
    trace1 'prev stage ' + prevStageIndx

    trace1 ' ' + xforms
    session.putAttribute(flowFile, 'xform.stage', '0')

    session.getProvenanceReporter().modifyContent(flowFile, "wsrhsrh")

    for (String xform : xforms) {
        //Stop processing flow file if it is suppressed at routing stage
        if (isFlowFileSuppressed) return null
        currStageIndx++
        flowFile.'xform.stage' = currStageIndx

        //Start process right after the previous stage
        if (currStageIndx > prevStageIndx) {
            trace1 'Stage ' + String.valueOf(currStageIndx)
            String[] nameParamsPair = xform.split('://')
            Map<String, String> params = null
            if (nameParamsPair.length > 1) {
                //Params must follow after ? symbol
                params = parseParams(nameParamsPair[1].substring(1))
            }
            for (Map.Entry<String, String> paramEntry : params.entrySet()) {
                trace 'Key ' + paramEntry.getKey() + ' val ' + paramEntry.getValue()
            }
            String name = nameParamsPair.length > 1 ? nameParamsPair[0] : xform
            trace 'processing ' + String.valueOf(currStageIndx) + ' stage'

            switch (name) {
                case('SyncResponse'):
                    syncResponse(flowFile)
                    break
                case("RouteOnContent"):
                    //Have to transfer a flow file to the special RouteOnContent processor group
                    String param = params.get('MatchRequirement')
                    if (!param) throw new IllegalArgumentException(name + ' ' + param)
                    flowFile.'content.match.strategy' = param
                    param = params.get('RouteCondition')
                    if (!param) throw new IllegalArgumentException(name + ' ' + param)
                    PropertyValue propValue = context.newPropertyValue(param)
                    String s = propValue.evaluateAttributeExpressions(flowFile).getValue()
                    flowFile.'route.on.content.condition' = s
                    param = params.get('Result')
                    propValue = context.newPropertyValue(param)
                    s = propValue.evaluateAttributeExpressions(flowFile).getValue()
                    flowFile.'route.on.content.result' = s
                    flowFile.'xform.group' = 'RouteOnContent'
                    isPropagated = true
                    break
                case("UpdateAttribute"):
                    //We have to support the Nifi EL in attributes
                    //So create temp hidden property to provide EL capabilities
                    for (Map.Entry<String, String> entry : params.entrySet()) {
                        PropertyValue propValue = context.newPropertyValue(entry.getValue())
                        String attrValue = propValue.evaluateAttributeExpressions(flowFile).getValue()
                        flowFile = session.putAttribute(flowFile, entry.getKey(), attrValue)
                    }
                    break
                case("RouteOnAttribute"):
                    String param = params.get('RoutingStrategy')
                    if (!param) throw new IllegalArgumentException(name + ' ' + param)
                    param = params.get('Condition')
                    if (!param) throw new IllegalArgumentException(name + ' ' + param)
                    PropertyValue propValue = context.newPropertyValue(param)
                    String res = propValue.evaluateAttributeExpressions(flowFile).getValue()
                    if (res == 'false') {
                        isFlowFileSuppressed = true
                        //throw new Exception('Result ' + res + ' does not match condition')
                    }
                    break
                case("ReplaceText"):
                    //Have to transfer a flow file to the special ReplaceText processor group
                    String replacementStrategy = params.get('ReplacementStrategy')
                    if (!replacementStrategy) throw new IllegalArgumentException(name + ' ' + param)
                    flowFile.'replace.text.mode' = replacementStrategy
                    String searchValue = params.get('SearchValue')
                    if (!searchValue) throw new IllegalArgumentException(name + ' ' + param)
                    PropertyValue propValue = context.newPropertyValue(searchValue)
                    searchValue = propValue.evaluateAttributeExpressions(flowFile).getValue()
                    flowFile.'replace.text.search.value' = searchValue
                    String replacementValue = params.get('ReplacementValue')
                    if (!replacementValue) throw new IllegalArgumentException(name + ' ' + param)
                    propValue = context.newPropertyValue(replacementValue)
                    replacementValue = propValue.evaluateAttributeExpressions(flowFile).getValue()
                    flowFile.'replace.text.replacement.value' = replacementValue
                    //flowFile.'xform.group' = 'ReplaceText'
                    //isPropagated = true
                    final int fileSize = (int) flowFile.getSize()
                    flowFile = replaceText(flowFile, replacementStrategy, searchValue,
                            replacementValue, 'EntireText', StandardCharsets.UTF_8, fileSize)
                    break
                case("EvaluateXQuery"):
                    String param = params.get('Destination')
                    if (param != 'flowfile-attribute') throw new IllegalArgumentException(name + ' ' + param)
                    params.remove('Destination')
                    for (Map.Entry<String, String> paramEntry : params.entrySet()) {
                        trace "Processing ${paramEntry.getKey()} "

                        if (paramEntry.getValue().indexOf('count(') > -1) {
                            trace '+count'
                            final StringBuilder sb = new StringBuilder()
                            session.read(flowFile, new InputStreamCallback(){

                                @Override
                                void process(InputStream is) throws IOException{
                                    def r = evaluateXPathValue(is,
                                            paramEntry.getValue().replace('\\\\', '\\'))
                                    sb.append(r)
                                }

                            })
                            session.putAttribute(flowFile, paramEntry.getKey(), sb.toString())
                        } else {
                            //final ByteArrayOutputStream baos = new ByteArrayOutputStream()
                            final List<String> list = new ArrayList<>()

                            session.read(flowFile, new InputStreamCallback(){

                                @Override
                                void process(InputStream is) throws IOException{
                                    List nodes = evaluateXPath(is,
                                            paramEntry.getValue().replace('\\\\', '\\'))

                                    for (Object node : nodes) {
                                        list.add(node)
                                    }

                                    /*for (int j = 0; j < nodes.getLength(); j++) {
                                        def node = nodes.item(j)
                                        list.add(node.toString())
                                    }*/
                                    //baos.write((evaluateXPath(is,
                                    //    paramEntry.getValue().replace('\\\\', '\\'))).getBytes())
                                }

                            })

                            //res = baos.toString()
                            trace1 '+res'
                            if (list.size() == 1) {
                                session.putAttribute(flowFile, paramEntry.getKey(), list.get(0))
                                trace1 "EvalXq res ${paramEntry.getKey()} " + list.get(0)
                            } else {
                                int sfx = 1
                                for (String s : list) {
                                    String attrName = paramEntry.getKey() + '.' + String.valueOf(sfx)
                                    trace1 "EvalXq res ${attrName} " + s
                                    session.putAttribute(flowFile, attrName, s)
                                    sfx++
                                }
                            }
                        }

                        //String r = Arrays.toString(res)
                    }
                    break
                case("ApplyXslt"):
                    final String param = params.get('Name')
                    if (!param) throw new IllegalArgumentException(name + ' ' + param)
                    //assert param != null
                    //ByteArrayInputStream fcCopy = stream2byteArray(flowFile)
                    //fc.reset()
                    flowFile = session.write(flowFile, new StreamCallback(){

                        @Override
                        void process(InputStream is, OutputStream os) throws IOException{
                            applyXslt(is, os, param)
                            os.flush()
                        }

                    })
                    break
                case("DuplicateFlowFile"):
                    /*flowFile.'xform.group' = name
                    isPropagated = true
                    break*/
                    String param = params.get('Number')
                    if (!param) throw new IllegalArgumentException(name + ' ' + param)
                    PropertyValue propValue = context.newPropertyValue(param)
                    param = propValue.evaluateAttributeExpressions(flowFile).getValue()
                    int numOfCopies = Integer.parseInt(param)
                    ArrayList<FlowFile> res = []

                    flowFile.'copy.index' = 0

                    if (currStageIndx == xforms.size() - 1) {
                        flowFile = session.removeAttribute(f, 'xform.group')
                    }
                    String ffid = flowFile.getAttribute('uuid')
                    for (int i = 0; i < numOfCopies; i++) {
                        FlowFile f = session.clone(flowFile)
                        f.'copy.index' = String.valueOf(i + 1)
                        graylogNotifyStart(f, ffid)
                        FlowFile ff = null
                        if (currStageIndx < xforms.size() - 1) {
                            ff = processXform(f, xforms, targetId)
                        }
                        if (ff == null) {
                            session.remove(f)
                        } else {
                            res.add(ff)
                        }
                    }
                    if (currStageIndx < xforms.size() - 1) {
                        ff = processXform(flowFile, xforms, targetId)
                        if (ff == null) {
                            session.remove(flowFile)
                        } else {
                            res.add(ff)
                        }
                    } else {
                        res.add(flowFile)
                    }
                    return res
                default:
                    for (Map.Entry<String, String> entry : params.entrySet()) {
                        flowFile = session.putAttribute(flowFile,
                                entry.getKey(), entry.getValue())
                    }
                    flowFile.'xform.group' = name
                    break
            }
            graylogNotify(flowFile, name)
        }
        if (isPropagated) {
            break
        }
    }
    trace 'Stage is ' + String.valueOf(currStageIndx) + ' size ' + String.valueOf(xforms.size())
    if (currStageIndx == xforms.size() - 1) {
        if (isPropagated) {
            if (!flowFile.getAttribute('target.output').equals('JSON')) {
                //Red pill for last xform to transfer flow file to the upload group
                flowFile.'xform.last' = 'true'
            }
        } else {
            if (flowFile.getAttribute('target.output').equals('JSON')) {
                flowFile = convertFlowFile(flowFile)
            }
            //Move right to upload group
            flowFile = session.removeAttribute(flowFile, 'xform.group')
        }
    }

    trace1 'FF stage ' + flowFile.getAttribute('xform.stage')
    return flowFile
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

FlowFile postprocessXform(FlowFile flowFile, boolean syncStatus, config) throws Exception{
    if (syncStatus) {
        trace "flowfile marked as SYNC, response flow name is: ${config.response}"
        flowFile.'iflow.status.code' = ''
        session.putAttribute(flowFile, 'business.process.name', config.response)
        //flowFileCopy.flow_name = it.response
    } //else {
    //flowFileCopy.'iflow.status.code' = getResponse(iflow.input, '200')
    //}
    trace 'Before potentional problem'
    if (flowFile == null) trace 'A eto null'
    flowFile.'iflow.input' = config.input
    //flowFile.'iflow.mapping' = config.mapping
    //if(it.id == "rdr") newFlowFile.'iflow.mapping' = "rdr/SKBP_Response"
    flowFile.'iflow.sync' = config.sync
    flowFile.'processGroupId' = '2fde38c3-b6b5-1fee-0c7c-7c06e1792e1a'

    //log.error context.getProperty(it.id).getValue()
    trace 'Prost'

    //trace "set target URL as system IP/DNS from parameter ${it.id}(value ${context.getProperty(it.id).getValue()}) and path ${j}"
    if (context.getProperty(config.id).getValue() == null) {
        log.error "Property for ${config.id} not found, add it to processor parameters!"
        //newFlowFile.'target_url' = "" + context.getProperty(it.id) + i
        flowFile.'target_system' = config.id
    }

    return flowFile
}

int findTarget(ArrayList targets, String id) {
    int i = -1
    for (def target : targets) {
        i++
        if (target.id == id) {
            return i
        }
    }
    return -1
}

def trace(String message) {
    traceOut += "\r\n+++++++ ${traceCount++} +++++++:" + message
//    log.log(LogLevel.INFO, message)
}

def trace1(String message) {
    traceOut1 += "\r\n+++++++ ${traceCount1++} +++++++:" + message
//    log.log(LogLevel.INFO, message)
}

void graylogNotify(FlowFile flowFile, String xformEntity) throws Exception{
    String sender = flowFile.getAttribute('http.query.param.senderService')
    if (!sender) {
        sender = 'Не указан'
    }
    String processGroupId = '60484390-d08c-1fe2-b9a9-d47458b352ee'
    String processGroupName = 'Transform'
    String hostName = InetAddress.getLocalHost().getHostName()
    String fileName = flowFile.getAttribute('filename')
    String uuid = flowFile.getAttribute('uuid')
    String pathVal = flowFile.getAttribute('path')
    String requestUri = flowFile.getAttribute('http.request.uri')
    if (requestUri.equals('/sap/xi')) {
        requestUri = flowFile.getAttribute('sap.Interface.value')
    }

    String xformPath = flowFile.getAttribute('xform.path')
    if (xformPath == null) {
        xformPath = 'unknow'
    }

    String xformStage = flowFile.getAttribute('xform.stage')
    if (xformStage == null) {
        xformStage = 'unknow'
    }

    //Определение получателя
    String receiver = "Не определен"
    def receiverLookup = receiverServiceId.asControllerService(StringLookupService)
    if (receiverLookup) {
        def coordinate = [key: requestUri]
        def value = receiverLookup.lookup(coordinate)
        if (value.isPresent()) {
            receiver = value.get()
        }
    }

    if (receiver.equals('attribute')) {
        receiver = flowFile.getAttribute('Receiver')
    }
    if (!receiver) {
        receiver = 'Не определен'
    }

    //Определение бизнес-процесса
    String businessProcessName = flowFile.getAttribute('business.process.name')
    /*def serviceLookup = clientServiceId.asControllerService(StringLookupService)
    if (serviceLookup) {
        def coordinate = [key: requestUri]
        def value = serviceLookup.lookup(coordinate)
        if (value.isPresent()) {
            businessProcessName = value.get()
        }
    }*/

    String specUrl = 'https://1desb-s-app01.gk.rosatom.local/nifi-docs/components/ru.greenatom.atombridge/af-specification-nar/4.0.0.0/ru.greenatom.af.Specifications/index.html#' +
            businessProcessName

    //Формирование GELF-сообщения
    String shortMessage = "Сообщение в [" + processGroupName + '] c filename [' + fileName + "], бизнес-процесс [" + businessProcessName + "], отправитель [" + sender + "], получатель [" + receiver + ']'

    def map = [_fileName: fileName, path: pathVal, short_message: shortMessage, host: hostName,
               facility: processGroupName, _groupId: processGroupId, level: 'INFO', _groupName: processGroupName,
               _messageUuid: uuid, _requestUrl: requestUri, _sender: sender, _receiver: receiver, _entryType: 'processing',
               _businessProcess: businessProcessName, specification: specUrl, transformationEntity: xformEntity, transformationPath: xformPath, transformationStage: xformStage]
    def json = groovy.json.JsonOutput.toJson(map)
    //Отправка GELF-сообщения
    def post = new URL('http://1tesb-s-grl01.gk.rosatom.local:12001/gelf').openConnection()
    post.setRequestMethod('POST')
    post.setDoOutput(true)
    post.setRequestProperty('Content-Type', 'application/json')
    post.getOutputStream().write(json.getBytes('UTF-8'))
    def postRC = post.getResponseCode()
    if (postRC < 200 && postRC > 300) {
        throw new Exception("Ошибка отправки, код " + postRC)
    }
}

void graylogNotifyStart(FlowFile flowFile, String derivationId) throws Exception{
    String sender = flowFile.getAttribute('http.query.param.senderService')
    if (!sender) {
        sender = 'Не указан'
    }
    String processGroupId = '60484390-d08c-1fe2-b9a9-d47458b352ee'
    String processGroupName = 'Transform'
    String hostName = InetAddress.getLocalHost().getHostName()
    String fileName = flowFile.getAttribute('filename')
    String uuid = flowFile.getAttribute('uuid')
    String pathVal = flowFile.getAttribute('path')
    String requestUri = flowFile.getAttribute('http.request.uri')
    if (requestUri.equals('/sap/xi')) {
        requestUri = flowFile.getAttribute('sap.Interface.value')
    }

    //Определение получателя
    String receiver = "Не определен"
    def receiverLookup = receiverServiceId.asControllerService(StringLookupService)
    if (receiverLookup) {
        def coordinate = [key: requestUri]
        def value = receiverLookup.lookup(coordinate)
        if (value.isPresent()) {
            receiver = value.get()
        }
    }

    if (receiver.equals('attribute')) {
        receiver = flowFile.getAttribute('Receiver')
    }
    if (!receiver) {
        receiver = 'Не определен'
    }

    //Определение бизнес-процесса
    String businessProcessName = flowFile.getAttribute('business.process.name')
    /*def serviceLookup = clientServiceId.asControllerService(StringLookupService)
    if (serviceLookup) {
        def coordinate = [key: requestUri]
        def value = serviceLookup.lookup(coordinate)
        if (value.isPresent()) {
            businessProcessName = value.get()
        }
    }*/

    String specUrl = 'https://1desb-s-app01.gk.rosatom.local/nifi-docs/components/ru.greenatom.atombridge/af-specification-nar/4.0.0.0/ru.greenatom.af.Specifications/index.html#' +
            businessProcessName

    //Формирование GELF-сообщения
    String shortMessage = "Сообщение в [" + processGroupName + '] c filename [' + fileName + "], бизнес-процесс [" + businessProcessName + "], отправитель [" + sender + "], получатель [" + receiver + ']'

    def map = [_fileName: fileName, path: pathVal, short_message: shortMessage, host: hostName,
               facility: processGroupName, _groupId: processGroupId, level: 'INFO', _groupName: processGroupName,
               _messageUuid: uuid, _requestUrl: requestUri, _sender: sender, _receiver: receiver, _entryType: 'start', _LogStart: 1, _LogSuccess: 0,
               _businessProcess: businessProcessName, specification: specUrl, derivation: derivationId]
    def json = groovy.json.JsonOutput.toJson(map)
    //Отправка GELF-сообщения
    def post = new URL('http://1tesb-s-grl01.gk.rosatom.local:12001/gelf').openConnection()
    post.setRequestMethod('POST')
    post.setDoOutput(true)
    post.setRequestProperty('Content-Type', 'application/json')
    post.getOutputStream().write(json.getBytes('UTF-8'))
    def postRC = post.getResponseCode()
    if (postRC < 200 && postRC > 300) {
        throw new Exception("Ошибка отправки, код " + postRC)
    }
}

FlowFile replaceText(final FlowFile flowFile, final String type, final String searchValue, final String replacementValue,
                     final String evaluateMode, final Charset charset, final int maxBufferSize) throws Exception{
    if (type.equals('RegexReplace')) {
        return regexReplaceText(flowFile, searchValue, replacementValue, evaluateMode, charset, maxBufferSize)
    } else {
        throw new Exception('Incorrect replace strategy')
    }
}

private static String normalizeReplacementString(String replacement) {
    String replacementFinal = replacement
    if (Pattern.compile('(\\$\\D)').matcher(replacement).find()) {
        replacementFinal = Matcher.quoteReplacement(replacement)
    }
    return replacementFinal
}

FlowFile regexReplaceText(final FlowFile flowFile, final String searchValue, final String replacementValue,
                          final String evaluateMode, final Charset charset, final int maxBufferSize) throws Exception{
    final int numCapturingGroups = Pattern.compile(searchValue).matcher('').groupCount()
    //final AttributeValueDecorator quotedAttributeDecorator = Pattern::quote

    final String searchRegex = searchValue
    final Pattern searchPattern = Pattern.compile(searchRegex)
    final Map<String, String> additionalAttrs = new HashMap<>(numCapturingGroups)

    FlowFile updatedFlowFile
    if (evaluateMode.equalsIgnoreCase('EntireText')) {
        final int flowFileSize = (int) flowFile.getSize()
        final int bufferSize = Math.min(maxBufferSize, flowFileSize)
        final byte[] buffer = new byte[bufferSize]

        session.read(flowFile,
                new InputStreamCallback(){

                    @Override
                    void process(InputStream is) throws IOException{
                        StreamUtils.fillBuffer(is, buffer, false)

                    }})

        final String contentString = new String(buffer, 0, flowFileSize, charset)
        final Matcher matcher = searchPattern.matcher(contentString)

        //final PropertyValue replacementValueProperty = replacementValue

        int matches = 0
        final StringBuffer sb = new StringBuffer()
        while (matcher.find()) {
            matches++

            for (int i = 0; i <= matcher.groupCount(); i++) {
                additionalAttrs.put('$' + i, matcher.group(i))
            }

            //String replacement = replacementValueProperty.evaluateAttributeExpressions(flowFile, additionalAttrs, escapeBackRefDecorator).getValue();
            //replacement = escapeLiteralBackReferences(replacement, numCapturingGroups);
            //String replacementFinal = normalizeReplacementString(replacement);
            String replacementFinal = normalizeReplacementString(replacementValue)

            matcher.appendReplacement(sb, replacementFinal)
        }

        if (matches > 0) {
            matcher.appendTail(sb)

            final String updatedValue = sb.toString()
            updatedFlowFile = session.write(flowFile, new OutputStreamCallback(){

                @Override
                void process(OutputStream os) throws IOException{
                    os.write(updatedValue.getBytes(charset))

                }})
        } else {
            return flowFile
        }
    } else throw new Exception('unsupported evaluation mode')
    return updatedFlowFile
}

void syncResponse(FlowFile flowFile) {
    FlowFile syncResponseFile = session.create(flowFile)
    session.putAttribute(syncResponseFile, 'sync.response', 'true')
    session.transfer(REL_SUCCESS, syncResponseFile)
}
