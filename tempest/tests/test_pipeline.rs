use tempest::common::now_millis;
use tempest::pipeline::*;
use tempest::source::SourceMsg;
use tempest::topology::{PipelineActor, TaskResponse};

fn get_source_msg(id: u8) -> SourceMsg {
    SourceMsg {
        id: vec![0, 0, id.clone()],
        msg: vec![id.clone()],
        ts: now_millis(),
    }
}

#[test]
fn test_pipeline() {
    // simulate messages being sent through the PipelineActor
    // define the pipeline

    /*
     */

    let pipeline = Pipeline::default()
        .add(Task::new("t1", "::T1"))
        .add(Task::new("t2", "::T2"))
        .add(Task::new("t3", "::T3"))
        .add(Task::new("t4", "::T4"))
        .add(Task::new("t5", "::T5"))
        .add(Task::new("t6", "::T6"))
        .add(Task::new("t7", "::T7"))
        .edge("t1", "t2")
        .edge("t1", "t3")
        .edge("t2", "t4")
        .edge("t3", "t4")
        .edge("t4", "t5")
        .edge("t4", "t6")
        .edge("t5", "t6")
        .edge("t5", "t7")
        .build();

    // println!("{:#?}", &pipeline);

    let mut pipeline_act = PipelineActor {
        pipeline: pipeline.clone(),
        // No timeout
        inflight: PipelineInflight::new(None),
        available: PipelineAvailable::new(&pipeline.tasks),
        aggregate: PipelineAggregate::new(&pipeline.tasks),
    };

    // define source msg
    let src_msg = get_source_msg(0);
    let src_msg_copy = src_msg.clone();

    // start task root
    pipeline_act.task_root(src_msg_copy);

    let result = pipeline_act.inflight.get(&vec![0, 0, 0]);
    assert_eq!(result.is_some(), true);

    if let Some((_ts, msg_state)) = result {
        println!("{:?}", msg_state);

        let pending = msg_state.get(&("root".to_owned(), "t1".to_owned()));
        assert_eq!(pending.is_some(), true);
        assert_eq!(pending.unwrap().len(), 1);
    };

    // we should have one message availbe on t1
    assert_eq!(pipeline_act.available.len(&"t1".to_owned()), Some(1usize));

    // now that the root task is pending
    // we need to simulate TaskResponse

    let task_resp = TaskResponse::Ack(
        src_msg.id.clone(),
        ("root".to_string(), "t1".to_string()),
        0,
        Some(vec![vec![1], vec![2]]),
    );

    pipeline_act.task_ack(task_resp);

    // t1, t2
    for index in 0..2 {
        let task_resp = TaskResponse::Ack(
            src_msg.id.clone(),
            ("t1".to_string(), "t2".to_string()),
            index,
            Some(vec![vec![1], vec![2]]),
        );
        pipeline_act.task_ack(task_resp);
    }

    // t1, t3
    for index in 0..2 {
        let task_resp = TaskResponse::Ack(
            src_msg.id.clone(),
            ("t1".to_string(), "t3".to_string()),
            index,
            Some(vec![vec![1], vec![2], vec![3]]),
        );
        pipeline_act.task_ack(task_resp);
    }

    // t2, t4
    for index in 0..4 {
        let task_resp = TaskResponse::Ack(
            src_msg.id.clone(),
            ("t2".to_string(), "t4".to_string()),
            index,
            None,
        );
        pipeline_act.task_ack(task_resp);
    }

    // t2, t4
    for index in 0..4 {
        let task_resp = TaskResponse::Ack(
            src_msg.id.clone(),
            ("t2".to_string(), "t4".to_string()),
            index,
            None,
        );
        pipeline_act.task_ack(task_resp);
    }

    // t3, t4
    for index in 0..6 {
        let task_resp = TaskResponse::Ack(
            src_msg.id.clone(),
            ("t3".to_string(), "t4".to_string()),
            index,
            None,
        );
        pipeline_act.task_ack(task_resp);
    }

    println!("{:?}", pipeline_act.inflight);
    println!("{:?}", pipeline_act.aggregate);
    println!("{:?}", pipeline_act.available);

    // println!("{:#?}", &pipeline_act.inflight.get(&src_msg.id));
    println!("{}", &pipeline_act.pipeline.to_graphviz());
}