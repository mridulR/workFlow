package com.mrdl.workflow.stage;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.mrdl.workflow.WorkFlowContext;
import com.mrdl.workflow.engine.*;
import com.mrdl.workflow.exception.RetryIngestionException;
import com.mrdl.workflow.retryer.DefaultWorkFlowRetryer;
import com.mrdl.workflow.retryer.WorkFlowRetryer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// CSOFF: MagicNumber
public class WorkFlowRunnerTest {

    private static final String BATCH_ID = "batchId";

    private WorkFlowRunner workFlowRunner;
    private WorkFlowContext workFlowContext;
    private WorkFlowRetryer workFlowRetryer;
    private WorkFlow workFlow;

    @Before
    public void setUp() {
        workFlowContext = mock(WorkFlowContext.class);
        when(workFlowContext.getDataBatchId()).thenReturn(BATCH_ID);
        workFlowRetryer = mock(DefaultWorkFlowRetryer.class);

        Retryer<StateExecutionResponse> retryer = RetryerBuilder.<StateExecutionResponse>newBuilder()
                .retryIfExceptionOfType(RetryIngestionException.class)
                .withWaitStrategy(WaitStrategies.fibonacciWait(100, 3, TimeUnit.MINUTES))
                .withStopStrategy(StopStrategies.stopAfterAttempt(3))
                .build();
        when(workFlowRetryer.getRetryer()).thenReturn(retryer);
    }

    @Test
    public void run_WhenAllStagesRunSuccessfully_thenReturnSuccessDataLoadResponse() throws Exception {

        workFlow = new WorkFlow() {
            @Override
            public List<WorkFlowStage> getWorkFlowStagesInSequence() {
                WorkFlowExecutionMonitor aspect = new WorkFlowExecutionMonitor();
                List<WorkFlowStage> workFlowStages = new LinkedList<>();
                workFlowStages.add((WorkFlowStage) workflowProxy(aspect, "stage1", true).getProxy());
                workFlowStages.add((WorkFlowStage) workflowProxy(aspect, "stage2", true).getProxy());
                workFlowStages.add((WorkFlowStage) workflowProxy(aspect, "stage3", true).getProxy());
                workFlowStages.add((WorkFlowStage) workflowProxy(aspect, "stage4", true).getProxy());
                return workFlowStages;
            }
        };

        workFlowRunner = new WorkFlowRunner() {
            @Override
            public WorkFlowRetryer getDataLoadRetryer() {
                return workFlowRetryer;
            }

            @Override
            public WorkFlow getWorkFlow() {
                return new WorkFlow() {
                    @Override
                    public List<WorkFlowStage> getWorkFlowStagesInSequence() {
                        return workFlowStageAspectAwareProxy(workFlow);
                    }
                };
            }
        };

        DataLoadResponse response = workFlowRunner.run(workFlowContext);

        assertThat(response.getDataLoadBatchId(), is(BATCH_ID));
        assertThat(response.getStatus(), is(Status.OK));
        assertThat(response.getStateLoadResponseMap().size(), is(4));
    }

    private List<WorkFlowStage> workFlowStageAspectAwareProxy(WorkFlow workflow) {
        List<WorkFlowStage> workFlowStages = new ArrayList<>();
        for (WorkFlowStage workFlowStage : workflow.getWorkFlowStagesInSequence()) {
            workFlowStages.add(workFlowAspectAwareProxy(workFlowStage));
        }
        return workFlowStages;
    }

    private WorkFlowStage workFlowAspectAwareProxy(WorkFlowStage workFlowStage) {
        AspectJProxyFactory factory = new AspectJProxyFactory(workFlowStage);
        WorkFlowExecutionMonitor aspect = new WorkFlowExecutionMonitor();
        factory.addAspect(aspect);
        return factory.getProxy();
    }

    private AspectJProxyFactory workflowProxy(
            WorkFlowExecutionMonitor workFlowExecutionMonitor, final String stageName, final boolean status) {

        AbstractWorkFlowStage abstractWorkFlowStage = new AbstractWorkFlowStage() {
            @Override
            public String getStageName() {
                return stageName;
            }

            @Override
            public StateExecutionResponse call() throws Exception {
                if (status) {
                    return StateExecutionResponse.builder().isSuccessful(status).build();
                } else {
                    return StateExecutionResponse.builder().isSuccessful(status).withError(
                            ErrorCode.ACCESS_DENIED, "Access Denied").build();
                }
            }
        };
        AspectJProxyFactory aspectJProxyFactory = new AspectJProxyFactory(abstractWorkFlowStage);
        aspectJProxyFactory.addAspect(workFlowExecutionMonitor);
        return aspectJProxyFactory;
    }

    @Test
    public void run_WhenSomeStageFailWithRetryExhaustException_thenReturnOKedDataLoadResponse() throws Exception {

        workFlow = new WorkFlow() {

            @Override
            public List<WorkFlowStage> getWorkFlowStagesInSequence() {

                WorkFlowExecutionMonitor aspect = new WorkFlowExecutionMonitor();
                List<WorkFlowStage> workFlowStages = new LinkedList<>();
                workFlowStages.add((WorkFlowStage) workflowProxy(aspect, "stage1", true).getProxy());
                workFlowStages.add((WorkFlowStage) workflowProxy(aspect, "stage2", false).getProxy());
                return workFlowStages;
            }
        };

        workFlowRunner = new WorkFlowRunner() {
            @Override
            public WorkFlowRetryer getDataLoadRetryer() {
                return workFlowRetryer;
            }

            @Override
            public WorkFlow getWorkFlow() {
                return new WorkFlow() {
                    @Override
                    public List<WorkFlowStage> getWorkFlowStagesInSequence() {
                        return workFlowStageAspectAwareProxy(workFlow);
                    }
                };
            }
        };


        DataLoadResponse response = workFlowRunner.run(workFlowContext);

        assertThat(response.getDataLoadBatchId(), is(BATCH_ID));
        assertThat(response.getStatus(), is(Status.OK));
        assertThat(response.getStateLoadResponseMap().size(), is(2));
        assertThat(response.getStateLoadResponseMap().get("stage2").getAttempt(), is(1));
    }

    //TODO: Fix them
    /*@Test
    public void run_WhenSomeStageFailWithRetryExceptionContinuously_thenReturnAbortedDataLoadResponse() throws Exception {

        workFlow = new WorkFlow() {

            private final AtomicInteger count = new AtomicInteger();

            @Override
            public List<WorkFlowStage> getWorkFlowStagesInSequence() {
                List<WorkFlowStage> workFlowStages = new LinkedList<>();

                workFlowStages.add(new AbstractWorkFlowStage() {
                    @Override
                    public String getStageName() {
                        return "stageOne";
                    }

                    @Override
                    public StateExecutionResponse call() throws Exception {
                        return StateExecutionResponse.builder().isSuccessful(true).build();
                    }
                });

                workFlowStages.add(new AbstractWorkFlowStage() {
                    @Override
                    public String getStageName() {
                        return "stageTwo";
                    }

                    @Override
                    public StateExecutionResponse call() throws Exception {
                        throw new RetryIngestionException("Ran " + count.incrementAndGet() + " times");
                    }
                });

                return workFlowStages;
            }
        };

        workFlowRunner = new WorkFlowRunner() {
            @Override
            public WorkFlowRetryer getDataLoadRetryer() {
                return workFlowRetryer;
            }

            @Override
            public WorkFlow getWorkFlow() {
                return new WorkFlow() {
                    @Override
                    public List<WorkFlowStage> getWorkFlowStagesInSequence() {
                        return workFlowStageAspectAwareProxy(workFlow);
                    }
                };
            }
        };


        DataLoadResponse response = workFlowRunner.run(workFlowContext);

        assertThat(response.getDataLoadBatchId(), is(BATCH_ID));
        assertThat(response.getStatus(), is(Status.ABORTED));
        assertThat(response.getStateLoadResponseMap().size(), is(3));
        assertThat(response.getStateLoadResponseMap().get("stageTwo").getStateExecutionResponse().isSuccessful(),
                is(false));
        assertThat(response.getStateLoadResponseMap().get("stageTwo").getStateExecutionResponse().getErrorCodes()
                .get(ErrorCode.RETRY_EXCEPTION), is("Ran 3 times"));
        assertThat(response.getStateLoadResponseMap().get("stageTwo").getAttempt(), is(3));
    }

    @Test
    public void run_WhenSomeStageFailWithRetryException_thenReturnSuccessDataLoadResponseAfterRetry() throws Exception {

        workFlow = new WorkFlow() {

            private final AtomicInteger count = new AtomicInteger();

            @Override
            public List<WorkFlowStage> getWorkFlowStagesInSequence() {
                List<WorkFlowStage> workFlowStages = new LinkedList<>();

                workFlowStages.add(new AbstractWorkFlowStage() {
                    @Override
                    public String getStageName() {
                        return "stageOne";
                    }

                    @Override
                    public StateExecutionResponse call() throws Exception {
                        return StateExecutionResponse.builder().isSuccessful(true).build();
                    }
                });

                workFlowStages.add(new AbstractWorkFlowStage() {
                    @Override
                    public String getStageName() {
                        return "stageTwo";
                    }

                    @Override
                    public StateExecutionResponse call() throws Exception {
                        if (count.incrementAndGet() < 3) {
                            throw new RetryIngestionException("Ran " + count.get() + " times");
                        }
                        return StateExecutionResponse.builder().isSuccessful(true).build();
                    }
                });

                return workFlowStages;
            }
        };

        workFlowRunner = new WorkFlowRunner() {
            @Override
            public WorkFlowRetryer getDataLoadRetryer() {
                return workFlowRetryer;
            }

            @Override
            public WorkFlow getWorkFlow() {
                return new WorkFlow() {
                    @Override
                    public List<WorkFlowStage> getWorkFlowStagesInSequence() {
                        return workFlowStageAspectAwareProxy(workFlow);
                    }
                };
            }
        };


        DataLoadResponse response = workFlowRunner.run(workFlowContext);

        assertThat(response.getDataLoadBatchId(), is(BATCH_ID));
        assertThat(response.getStatus(), is(Status.OK));
        assertThat(response.getStateLoadResponseMap().size(), is(4));
        assertThat(response.getStateLoadResponseMap().get("validate").getStateExecutionResponse().isSuccessful(),
                is(true));
        assertThat(response.getStateLoadResponseMap().get("validate").getAttempt(), is(3));
    }*/
}