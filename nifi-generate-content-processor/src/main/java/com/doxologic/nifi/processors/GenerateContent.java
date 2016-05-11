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
package com.doxologic.nifi.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

@Tags({"flowfile", "generate", "modification", "text", "Expression Language"})
@CapabilityDescription("Creates a new FlowFile content based on property value.")
@SeeAlso(classNames = {"GenerateFlowFile"})
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
public class GenerateContent extends AbstractProcessor {

    private static final PropertyDescriptor CONTENT = new PropertyDescriptor
            .Builder().name("Content")
            .description("The content for the generated FlowFile.")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();
    private static final PropertyDescriptor COPY_ATTRIBUTES = new PropertyDescriptor
            .Builder().name("Copy Attributes")
            .description("Copy attributes from original FlowFile?")
            .required(false)
            .addValidator(Validator.VALID)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    private static final Relationship CONVERTED = new Relationship.Builder()
            .name("converted")
            .build();
    private static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(CONTENT);
        descriptors.add(COPY_ATTRIBUTES);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(CONVERTED);
        relationships.add(ORIGINAL);
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile originalFlowFile = session.get();
        FlowFile newFlowFile;
        if (originalFlowFile != null) {
            newFlowFile = session.clone(originalFlowFile);
            session.getProvenanceReporter().clone(originalFlowFile, newFlowFile);
            session.transfer(originalFlowFile, ORIGINAL);

            if ("true".equals(context.getProperty(COPY_ATTRIBUTES).getValue())) {
                newFlowFile = session.putAllAttributes(newFlowFile, originalFlowFile.getAttributes());
            }
        } else {
            newFlowFile = session.create();
        }

        final String content = context.getProperty(CONTENT).evaluateAttributeExpressions(originalFlowFile).getValue();
        newFlowFile = session.write(newFlowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                out.write(content.getBytes("UTF-8"));
            }
        });
        session.transfer(newFlowFile, CONVERTED);
        session.commit();
    }
}
