use tempest::actix::*;
use tempest::common::now_millis;
use tempest::metric::Metrics;
use tempest::pipeline::*;
use tempest::source::SourceMsg;
use tempest::task;
use tempest::topology::{PipelineActor, TaskResponse};

fn get_source_msg(id: u8) -> SourceMsg {
    SourceMsg {
        id: vec![0, 0, id.clone()],
        msg: vec![id.clone()],
        ts: now_millis(),
        delivered: 0,
    }
}

fn get_pipeline() -> Pipeline {
    Pipeline::default()
        .task(T1::default())
        .task(T2::default())
        .task(T3::default())
        .task(T4::default())
        .task(T5::default())
        .task(T6::default())
        .task(T7::default())
        .edge("t1", "t2")
        .edge("t1", "t3")
        .edge("t2", "t4")
        .edge("t3", "t4")
        .edge("t4", "t5")
        .edge("t4", "t6")
        .edge("t5", "t6")
        .edge("t5", "t7")
        .build()
}

#[derive(Default, Debug)]
pub struct T1 {}
impl task::Task for T1 {
    fn name(&self) -> &'static str {
        "t1"
    }
}

#[derive(Default, Debug)]
pub struct T2 {}
impl task::Task for T2 {
    fn name(&self) -> &'static str {
        "t2"
    }
}

#[derive(Default, Debug)]
pub struct T3 {}
impl task::Task for T3 {
    fn name(&self) -> &'static str {
        "t3"
    }
}

#[derive(Default, Debug)]
pub struct T4 {}
impl task::Task for T4 {
    fn name(&self) -> &'static str {
        "t4"
    }
}

#[derive(Default, Debug)]
pub struct T5 {}
impl task::Task for T5 {
    fn name(&self) -> &'static str {
        "t5"
    }
}

#[derive(Default, Debug)]
pub struct T6 {}
impl task::Task for T6 {
    fn name(&self) -> &'static str {
        "t6"
    }
}

#[derive(Default, Debug)]
pub struct T7 {}
impl task::Task for T7 {
    fn name(&self) -> &'static str {
        "t7"
    }
}

#[derive(Default, Debug)]
pub struct TaskRoot {}
impl task::Task for TaskRoot {
    fn name(&self) -> &'static str {
        "root"
    }
}

#[test]
#[should_panic]
fn test_pipeline_cycle() {
    let _ = Pipeline::default()
        .task(T1::default())
        .task(T2::default())
        .task(T3::default())
        .edge("t1", "t2")
        .edge("t2", "t3")
        .edge("t3", "t1")
        .build();
}

#[test]
#[should_panic]
fn test_pipeline_task_root() {
    let _ = Pipeline::default().task(TaskRoot::default()).build();
}

#[test]
#[should_panic]
fn test_pipeline_same_edge() {
    let _ = Pipeline::default()
        .task(T1::default())
        .task(T2::default())
        .edge("t1", "t1")
        .build();
}

#[test]
#[should_panic]
fn test_pipeline_undefined_task_edge() {
    let _ = Pipeline::default()
        .task(T1::default())
        .task(T2::default())
        .edge("Y1", "Y2")
        .build();
}

#[test]
fn test_pipeline() {
    // simulate messages being sent through the PipelineActor
    // define the pipeline

    let _ = System::new("Task");

    let pipeline = get_pipeline();

    // println!("{:#?}", &pipeline);
    let mut pipeline_act = PipelineActor {
        pipeline: pipeline.runtime(),
        inflight: PipelineInflight::new(None),
        available: PipelineAvailable::new(pipeline.names()),
        aggregate: PipelineAggregate::new(pipeline.names()),
        metrics: Metrics::default().named(vec!["pipeline"]),
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

    // we should have one message available on t1
    assert_eq!(pipeline_act.available.len(&"t1".to_owned()), 1usize);

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
