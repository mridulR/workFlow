package com.mrdl.workflow.engine;



import java.util.List;

public interface WorkFlow {

    List<WorkFlowStage> getWorkFlowStagesInSequence();
}
