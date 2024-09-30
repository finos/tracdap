/**
 * A component that shows information about the environment being used and any details added in the trac-platform.yaml
 * configuration file. This is so that the user knows exactly where the data imported will be saved.
 * @module
 * @category BatchDataImportScene component
 */

import {Alert} from "../../../components/Alert";
import Col from "react-bootstrap/Col";
import {DeploymentInfoTable} from "../../../components/DeploymentInfoTable";
import React from "react";
import Row from "react-bootstrap/Row";
import {useAppSelector} from "../../../../types/types_hooks";

// Some Bootstrap grid layouts
const xlGrid = {span: 10, offset: 1}
const xsGrid = {span: 12, offset: 0}

export const EnvironmentNotice = () => {

    // Get what we need from the store
    const {production, environment} = useAppSelector(state => state["applicationStore"].platformInfo)
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    return (

        <Row>
            <Col className={"pt-3"} xs={xsGrid} xl={xlGrid}>

                {searchAsOf &&
                    <Alert variant={"warning"}>
                        You will not be able to batch import data in time travel mode.
                    </Alert>
                }

                <Alert className={"mt-4 py-3"} showBullets={false} variant={production === true ? "danger" : "success"}>
                    You are using the {environment ? environment : "unknown"} platform, all batch imports will be loaded into this
                    version. {production === true ? "This is a PRODUCTION environment." : ""}
                </Alert>

                <DeploymentInfoTable showTitle={true}/>
            </Col>
        </Row>
    )
};