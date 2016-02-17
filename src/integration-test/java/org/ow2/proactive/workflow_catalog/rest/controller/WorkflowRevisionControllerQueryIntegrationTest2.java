/*
 * ProActive Parallel Suite(TM): The Java(TM) library for
 *    Parallel, Distributed, Multi-Core Computing for
 *    Enterprise Grids & Clouds
 *
 * Copyright (C) 1997-2016 INRIA/University of
 *                 Nice-Sophia Antipolis/ActiveEon
 * Contact: proactive@ow2.org or contact@activeeon.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation; version 3 of
 * the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 *
 * Initial developer(s):               The ProActive Team
 *                         http://proactive.inria.fr/team_members.htm
 */
package org.ow2.proactive.workflow_catalog.rest.controller;

import java.io.IOException;
import java.util.Optional;

import javax.xml.stream.XMLStreamException;

import org.ow2.proactive.workflow_catalog.rest.Application;
import org.ow2.proactive.workflow_catalog.rest.dto.BucketMetadata;
import org.ow2.proactive.workflow_catalog.rest.dto.WorkflowMetadata;
import org.ow2.proactive.workflow_catalog.rest.util.ProActiveWorkflowParserResult;
import com.google.common.collect.ImmutableMap;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.response.ValidatableResponse;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static com.jayway.restassured.RestAssured.given;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

/**
 * @author ActiveEon Team
 */
@ActiveProfiles("test")
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = { Application.class })
@WebIntegrationTest
public class WorkflowRevisionControllerQueryIntegrationTest2 extends AbstractWorkflowRevisionControllerTest {

    private Logger log = LoggerFactory.getLogger(WorkflowRevisionControllerQueryIntegrationTest2.class);

    private BucketMetadata bucket1;

    private BucketMetadata bucket2;

    @Before
    public void setup() throws IOException, XMLStreamException {
        bucket1 = bucketService.createBucket("bucket1");
        bucket2 = bucketService.createBucket("bucket2");

        // Workflow A
        ImmutablePair<WorkflowMetadata, ProActiveWorkflowParserResult> workflow =
                createWorkflow("",
                        "A",
                        ImmutableMap.of("I", "E", "type", "public"), ImmutableMap.of("CPU", "4"));

        workflowRevisionService.createWorkflowRevision(
                bucket1.id, Optional.of(workflow.getLeft().id), workflow.getRight(), new byte[0]);

        // Workflow B
        workflow = createWorkflow("", "B", ImmutableMap.of("C", "E", "type", "private"),
                ImmutableMap.of("CPU", "2"));

        workflowRevisionService.createWorkflowRevision(
                bucket1.id, Optional.of(workflow.getLeft().id), workflow.getRight(), new byte[0]);

        // Workflow C
        createWorkflow("", "C", ImmutableMap.of("I", "O"), ImmutableMap.of());

        // Workflow D
        createWorkflow("", "D", ImmutableMap.of(), ImmutableMap.of("CPU", "5"));

        // Workflow E
        createWorkflow("Fabien",
                "E",
                ImmutableMap.of("toto", "Google", "tata", "Amazon"),
                ImmutableMap.of("toto", "Google", "tata", "Amazon"));

        // Workflow F
        createWorkflow("Fabien", "F", ImmutableMap.of("toto", "Amazon"), ImmutableMap.of("toto", "Amazon"));

        // Workflow G
        createWorkflow("Strange", "G%", ImmutableMap.of("linebreak", "\\r\\n"), ImmutableMap.of());

        // Workflow H
        createWorkflow("", "Amazon", ImmutableMap.of(), ImmutableMap.of());

        // Workflow that belongs to bucket 2

        createWorkflow(bucket2, "", "Dummy", ImmutableMap.of(), ImmutableMap.of());

        createWorkflow(bucket2, "",
                "A", ImmutableMap.of("I", "E", "type", "public"), ImmutableMap.of("CPU", "4"));
    }

    private ImmutablePair<WorkflowMetadata, ProActiveWorkflowParserResult> createWorkflow(
            String projectName, String name,
            ImmutableMap<String, String> genericInformation, ImmutableMap<String, String> variable) {
        return createWorkflow(bucket1, projectName, name, genericInformation, variable);
    }

    private ImmutablePair<WorkflowMetadata, ProActiveWorkflowParserResult> createWorkflow(BucketMetadata bucket,
            String projectName, String name,
            ImmutableMap<String, String> genericInformation, ImmutableMap<String, String> variable) {

        ProActiveWorkflowParserResult proActiveWorkflowParserResult =
                new ProActiveWorkflowParserResult(projectName, name,
                        genericInformation, variable);

        WorkflowMetadata workflow = workflowService.createWorkflow(
                bucket.id, proActiveWorkflowParserResult, new byte[0]);

        return ImmutablePair.of(workflow, proActiveWorkflowParserResult);
    }

    @Test
    public void test() {
        String query1 = "generic_information(\"I\",\"E\")"; // result -> A
        String query2 = "generic_information(\"I\", \"E\") OR generic_information(\"C\", \"E\")"; // result -> A, B
        String query3 = "variable(\"CPU\", \"5%\")"; // result -> D
        String query4 = "generic_information(\"I\", \"E\") OR generic_information(\"C\", \"E\") AND variable(\"CPU\", \"%\")"; // -> A, B
        String query5 = "variable(\"toto\", \"%\")"; // result -> E,F
        String query6 = "variable(\"toto\",\"Amazon\")"; // result -> F
        String query7 = "generic_information(\"I\", \"E\") AND generic_information(\"C\", \"E\") OR variable(\"CPU\", \"%\")"; // -> A, B, D
        String query8 = "variable(\"CPU\", \"%\") AND name=\"B\" AND generic_information(\"C\", \"E\")"; // -> B
        String query9 = "project_name=\"\""; // A, B, C, D
        String query10 = "project_name=\"%ien\""; // E, F
        String query11 = "project_name=\"Fab%\""; // E, F
        String query12 = "project_name=\"%b%\""; // E, F
        String query13 = "project_name=\"%oo%\""; // E, F
        String query14 = "name=\"G\\%\""; // G
        String query15 = "generic_information(\"linebreak\", \"\\r\\n\")"; // G
        String query16 = "variable(\"%\", \"%\")"; // A B D E F
        String query17 = "generic_information(\"I\", \"E\") AND generic_information(\"type\", \"public\") " +
                "AND variable(\"CPU\", \"%\") OR generic_information(\"C\", \"E\") " +
                "AND variable(\"CPU\", \"%\") OR name=\"Amazon\""; // A, B, Amazon
        String query18 = "";
        String query19 = "name=\"A\"";



        ValidatableResponse mostRecentWorkflowRevisions =
                findMostRecentWorkflowRevisions(query19);
    }

    public ValidatableResponse findMostRecentWorkflowRevisions(String wcqlQuery) {
        Response response = given().pathParam("bucketId", bucket1.id)
                .queryParam("size", 100)
                .queryParam("query", wcqlQuery)
                .when().get(WORKFLOWS_RESOURCE);

        logQueryAndResponse(wcqlQuery, response);

        return response.then().assertThat();
    }

    public ValidatableResponse findAllWorkflowRevisions(String wcqlQuery, long workflowId) {
        Response response = given().pathParam("bucketId", bucket1.id).pathParam("workflowId", workflowId)
                .queryParam("size", 100)
                .queryParam("query", wcqlQuery)
                .when().get(WORKFLOW_REVISIONS_RESOURCE);

        logQueryAndResponse(wcqlQuery, response);

        return response.then().assertThat();
    }

    private void logQueryAndResponse(String wcqlQuery, Response response) {
        log.info("WCQL query used is '{}'", wcqlQuery);
        log.info("Response is:\n{}", prettify(response.asString()));
    }

}