/**
 * A component that shows information about an imported csv or Excel file. If the dataset has already been
 * found in TRAC then a message is also shown with a link to the dataset.
 * @module SelectedDataInfo
 * @category Component
 */

import {Alert} from "../../../components/Alert";
import Col from "react-bootstrap/Col";
import {FileDetails} from "../../../components/FileDetails";
import {General} from "../../../../config/config_general";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {Link} from "react-router-dom";
import {ObjectSummaryPaths} from "../../../../config/config_menu";
import React from "react";
import Row from "react-bootstrap/Row";
import {useAppSelector} from "../../../../types/types_hooks";

// Some Bootstrap grid layouts
const lgGrid = {span: 10, offset: 1}
const xlGrid = {span: 8, offset: 2}

export const SelectedDataInfo = () => {

    // Get what we need from the store
    const {fileInfo} = useAppSelector(state => state["uploadADatasetStore"].import)
    const {foundInTrac, tag} = useAppSelector(state => state["uploadADatasetStore"].alreadyInTrac)

    console.log(fileInfo)
    return (
        <React.Fragment>
            {fileInfo &&
                <Row>
                    <Col xs={12} lg={lgGrid} xl={xlGrid}>
                        <HeaderTitle type={"h3"} text={"File details"}/>
                        <FileDetails fileInfo={fileInfo}/>
                    </Col>

                    <Col xs={12} lg={lgGrid} xl={xlGrid}>

                        {!General.loading.allowCopies.data && foundInTrac && tag?.objectId && tag.objectType &&

                            <Alert className={"my-3"} variant={"warning"}>
                                <div>
                                    This file has already been loaded into TRAC, the object ID of the data
                                    is <Link className={"alert-link"}
                                          to={`${ObjectSummaryPaths[tag.objectType].to}/${tag.objectId}/${tag.objectVersion}/${tag.tagVersion}/`}
                                    >{tag.objectId}</Link>. This file can only be uploaded
                                    again if you need to change the field names or types of the dataset.
                                </div>
                            </Alert>
                        }

                    </Col>
                </Row>
            }

        </React.Fragment>
    )
};