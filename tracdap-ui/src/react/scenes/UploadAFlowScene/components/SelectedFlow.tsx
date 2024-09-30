import {Alert} from "../../../components/Alert";
import Col from "react-bootstrap/Col";
import {FileDetails} from "../../../components/FileDetails";
import {General} from "../../../../config/config_general";
import {HeaderTitle} from "../../../components/HeaderTitle";
import React from "react";
import Row from "react-bootstrap/Row";
import {useAppSelector} from "../../../../types/types_hooks";
import {SelectValue} from "../../../components/SelectValue";
import {tracdap as trac} from "@finos/tracdap-web-api";

// Some Bootstrap grid layouts
const lgGrid = {span: 10, offset: 1}
const xlGrid = {span: 8, offset: 2}

/**
 * A component that shows information about an imported csv or Excel file and a table of the data loaded.
 */

const SelectedFLow = () => {

    // Get what we need from the store
    const {fileInfo} = useAppSelector(state => state["uploadAFlowStore"].import)
    const {data} = useAppSelector(state => state["uploadAFlowStore"].file)
    const {foundInTrac, tag} = useAppSelector(state => state["uploadAFlowStore"].alreadyInTrac)

    return (
        <React.Fragment>
            {fileInfo &&
                <Row>
                    <Col xs={12} lg={lgGrid} xl={xlGrid}>
                        <HeaderTitle type={"h3"} text={"File details"}/>
                        <FileDetails fileInfo={fileInfo}/>
                    </Col>

                    <Col xs={12} lg={lgGrid} xl={xlGrid}>

                        {!General.loading.allowCopies.data && foundInTrac && tag &&
                            <Alert className={"my-3"} variant={"warning"}>
                                {/*TODO add a link that says you can view this dataset here*/}
                                <div>
                                    This file has already been loaded into
                                    TRAC, the object ID of the flow is <span
                                    className={"user-select-all"}>{tag.objectId}</span>.
                                </div>
                            </Alert>
                        }

                    </Col>
                </Row>
            }

            {data &&
                <React.Fragment>
                    <HeaderTitle type={"h3"} text={"Imported flow"}/>

                    <SelectValue basicType={trac.STRING}
                                 mustValidate={false}
                                 onChange={() => {
                                 }}
                                 readOnly={true}
                                 rows={40}
                                 specialType={"TEXTAREA"}
                                 validateOnMount={false}
                                 value={data}
                    />
                </React.Fragment>
            }
        </React.Fragment>
    )
};

export default SelectedFLow;