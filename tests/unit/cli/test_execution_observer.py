import logging

from datapipeline.cli.visuals.execution import HierarchicalExecutionObserver
from datapipeline.cli.visuals.execution_context import current_dag_depth
from datapipeline.dag.events import DagRunEvent, NodeRunEvent


def test_hierarchical_observer_indents_nested_dags(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test")
    observer = HierarchicalExecutionObserver(logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="outer", node_count=2)
        observer.on_dag_start(dag_name="inner", node_count=1)
        observer.on_dag_end(
            DagRunEvent(
                dag_name="inner",
                node_count=1,
                output_items=3,
                elapsed_seconds=0.01,
                status="success",
            )
        )
        observer.on_dag_end(
            DagRunEvent(
                dag_name="outer",
                node_count=2,
                output_items=3,
                elapsed_seconds=0.02,
                status="success",
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert messages[0].startswith("DAG started name=outer")
    assert messages[1].startswith("  DAG started name=inner")
    assert messages[2].startswith("  DAG finished name=inner")
    assert messages[3].startswith("DAG finished name=outer")



def test_hierarchical_observer_logs_node_events_at_debug(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.nodes")
    observer = HierarchicalExecutionObserver(logger)

    with caplog.at_level(logging.DEBUG, logger=logger.name):
        observer.on_dag_start(dag_name="demo", node_count=1)
        observer.on_node_start(dag_name="demo", node_name="n", stage=0)
        observer.on_node_end(
            NodeRunEvent(
                dag_name="demo",
                node_name="n",
                stage=0,
                output_items=2,
                elapsed_seconds=0.01,
                status="success",
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert any(msg.startswith("  Node started dag=demo") for msg in messages)
    assert any(msg.startswith("  Node finished dag=demo") for msg in messages)



def test_hierarchical_observer_updates_context_depth():
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.depth")
    observer = HierarchicalExecutionObserver(logger)

    assert current_dag_depth() == 0
    observer.on_dag_start(dag_name="outer", node_count=1)
    assert current_dag_depth() == 1
    observer.on_dag_start(dag_name="inner", node_count=1)
    assert current_dag_depth() == 2
    observer.on_dag_end(
        DagRunEvent(
            dag_name="inner",
            node_count=1,
            output_items=0,
            elapsed_seconds=0.0,
            status="success",
        )
    )
    assert current_dag_depth() == 1
    observer.on_dag_end(
        DagRunEvent(
            dag_name="outer",
            node_count=1,
            output_items=0,
            elapsed_seconds=0.0,
            status="success",
        )
    )
    assert current_dag_depth() == 0


def test_hierarchical_observer_keeps_siblings_flat_and_parent_finishes_last(caplog):
    logger = logging.getLogger("datapipeline.cli.visuals.execution.test.siblings")
    observer = HierarchicalExecutionObserver(logger)

    with caplog.at_level(logging.INFO, logger=logger.name):
        observer.on_dag_start(dag_name="vector:assemble", node_count=2)
        observer.on_dag_start(dag_name="feature:linear_time", node_count=9)
        observer.on_dag_start(dag_name="feature:closing_price", node_count=9)

        observer.on_dag_end(
            DagRunEvent(
                dag_name="vector:assemble",
                node_count=2,
                output_items=23,
                elapsed_seconds=9.0,
                status="success",
            )
        )
        observer.on_dag_end(
            DagRunEvent(
                dag_name="feature:closing_price",
                node_count=9,
                output_items=33,
                elapsed_seconds=8.0,
                status="success",
            )
        )
        observer.on_dag_end(
            DagRunEvent(
                dag_name="feature:linear_time",
                node_count=9,
                output_items=29,
                elapsed_seconds=9.5,
                status="success",
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert messages[0].startswith("DAG started name=vector:assemble")
    assert messages[1].startswith("  DAG started name=feature:linear_time")
    assert messages[2].startswith("  DAG started name=feature:closing_price")
    assert messages[3].startswith("  DAG finished name=feature:closing_price")
    assert messages[4].startswith("  DAG finished name=feature:linear_time")
    assert messages[5].startswith("DAG finished name=vector:assemble")
    assert current_dag_depth() == 0
