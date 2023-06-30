/**
 * This scene shows a range of chart options, parameter and attribute widgets.
 * @module ExampleScene
 * @category Scene
 */

import {ChartTs as Chart} from "../../components/Chart/Chart-ts";
import Col from "react-bootstrap/Col";
import React from "react";
import Row from "react-bootstrap/Row";
import {SceneTitle} from "../../components/SceneTitle";
import Tab from "react-bootstrap/Tab";
import Tabs from "react-bootstrap/Tabs";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the ExamplesScene index component.
 */
export interface Props {

    /**
     * The main title for the page, this is set in the {@link MenuConfig}.
     */
    title: string
}

export const ExamplesScene = (props: Props) => {

    console.log("Rendering ExamplesScene")

    const {title} = props;

    // The object tags for the rows selected in the table
    // const {inputs, charts} = useAppSelector(state => state["examplesStore"])

    return (

        <React.Fragment>

            <SceneTitle text={title}/>

            <Tabs id={"examples"} defaultActiveKey="charts" className={"mt-4"}>
                <Tab className={"tab-content-bordered pt-4 pb-4 min-height-tab-pane"} title={"Charts"} eventKey={"charts"}>

                    <Row>
                        {/*{charts.map((chart, i) =>*/}
                        {/*    <Col md={6} lg={6} key={i}>*/}
                        {/*        <Chart  {...chart}*/}
                        {/*                data={inputs[chart.input].data}*/}
                        {/*                fields={inputs[chart.input].fields}*/}
                        {/*                headerComponent={<span className={"text-secondary"}>{chart.savedChartState.title}</span>}*/}
                        {/*        />*/}
                        {/*    </Col>*/}
                        {/*)}*/}
                    </Row>
                </Tab>

                <Tab className={"tab-content-bordered pb-4 min-height-tab-pane"} title={"Bullets & Gauges"} eventKey={"bulletsAndGauges"}>

                    {/*<Row>*/}

                    {/*    <Col md={6} lg={4}>*/}

                    {/*        <Bullet value={19}*/}
                    {/*                valueSchema={{formatCode: ",|.|1|£|bn", fieldType: "FLOAT"}}*/}
                    {/*                targetSchema={{formatCode: ",|.|0|£|bn"}}*/}
                    {/*                title={"Example 1"}*/}
                    {/*                target={20}*/}
                    {/*                stops={{green: 0, amber: 20, red: 70}}*/}
                    {/*                spacing={{top: 50, bottom: 50}}*/}
                    {/*                chartHeight={50}*/}
                    {/*        />*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={4}>*/}

                    {/*        <Gauge value={9}*/}
                    {/*               schema={{formatCode: ",|.|0|£|bn"}}*/}
                    {/*               title={"Example 2"}*/}
                    {/*               stops={{green: 0, amber: 10, red: 70}}*/}
                    {/*        />*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={4}>*/}

                    {/*        <Bullet value={20}*/}
                    {/*                title={"Example 3"}*/}
                    {/*                valueSchema={{formatCode: ",|.|0|£|bn"}}*/}
                    {/*                stops={{green: 0, amber: 20, red: 70}}*/}
                    {/*                spacing={{top: 50, bottom: 50}}*/}
                    {/*                chartHeight={50}*/}
                    {/*        />*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={3}>*/}

                    {/*        <Bullet*/}
                    {/*            title={"Example 4"}/>*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={3}>*/}

                    {/*        <Bullet value={60}*/}
                    {/*                title={"Example 5"}/>*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={3}>*/}

                    {/*        <Bullet value={70}*/}
                    {/*                title={"Example 6"}/>*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={3}>*/}

                    {/*        <Bullet value={80}*/}
                    {/*                title={"Example 7"}/>*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={6}>*/}

                    {/*        <Bullet value={90}*/}
                    {/*                title={"Example 8"}/>*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={6}>*/}

                    {/*        <Bullet value={180}*/}
                    {/*                title={"Example 9"}/>*/}
                    {/*    </Col>*/}

                    {/*</Row>*/}

                    {/*<Row>*/}

                    {/*    <Col md={6} lg={4}>*/}

                    {/*        <Gauge value={9}*/}
                    {/*               schema={{formatCode: ",|.|0|£|bn"}}*/}
                    {/*               title={"Example 10"}*/}
                    {/*               stops={{green: 0, amber: 10, red: 70}}*/}
                    {/*        />*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={4}>*/}

                    {/*        <Gauge value={10}*/}
                    {/*               title={"Example 11"}*/}
                    {/*               schema={{formatCode: ",|.|0|£|bn"}}*/}
                    {/*               stops={{green: 0, amber: 10, red: 70}}*/}
                    {/*        />*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={4}>*/}

                    {/*        <Gauge value={70}*/}
                    {/*               title={"Example 12"}*/}
                    {/*               stops={{green: 0, amber: 10, red: 70}}*/}
                    {/*        />*/}
                    {/*    </Col>*/}
                    {/*    <Col md={6} lg={3}>*/}

                    {/*        <Gauge value={40.556}*/}
                    {/*               title={"Example 13"}*/}
                    {/*        />*/}
                    {/*    </Col>*/}

                    {/*</Row>*/}

                </Tab>
            </Tabs>
        </React.Fragment>
    )
};

// This additional export is needed to be able to load this scene lazily in config_menu.tsx
export default ExamplesScene;