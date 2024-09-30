import {addNewPriorVersionOption, setPriorVersionOption} from "../store/uploadADatasetStore";
import {Alert} from "../../../components/Alert";
import Col from "react-bootstrap/Col";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {MetadataViewer} from "../../../components/MetadataViewer/MetadataViewer";
import React from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../../../components/SelectOption";
import {ShowHideDetails} from "../../../components/ShowHideDetails";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../../types/types_hooks";

// Some Bootstrap grid layouts
const lgGrid = {span: 10, offset: 1}
const xlGrid = {span: 8, offset: 2}

/**
 * A component that allows the user to enter an object ID or an object key of a dataset and set it as the object that
 * the imported dataset will update, this will create a new version. Several checks are needed to ensure that this will
 * work without error. The object ID or an object key needs to be for a dataset and that dataset must have a schema
 * that has the same matching variables (TRAC allows new versions to have more variables that the previous versions).
 */

const SetPriorVersion = () => {

    // Get what we need from the store
    const {status: importStatus} = useAppSelector(state => state["uploadADatasetStore"].import)
    const {
        selectedTab
    } = useAppSelector(state => state["uploadADatasetStore"].existingSchemas)
    const {
        selectedOption,
        options,
        isPriorVersionSchemaTheSame
    } = useAppSelector(state => state["uploadADatasetStore"].priorVersion)

    return (
        <React.Fragment>
            {importStatus !== "idle" &&

                <React.Fragment>
                    <HeaderTitle type={"h3"} text={`Update an existing dataset`}/>

                    <TextBlock>
                        You can opt to upload the dataset as an update to an existing version. The previous version will
                        not be deleted and will still available to be selected as an input to a flow. This is only
                        recommended however if there was an issue or error with the previous version of the dataset.
                    </TextBlock>

                    <Row className={"pb-3"}>
                        <Col xs={12} lg={lgGrid} xl={xlGrid} className={"my-3"}>
                            <SelectOption basicType={trac.STRING}
                                          hideDropdown={true}
                                          isClearable={true}
                                          isCreatable={true}
                                          objectType={trac.ObjectType.DATA}
                                          onChange={setPriorVersionOption}
                                          onCreateNewOption={addNewPriorVersionOption}
                                          options={options}
                                          placeHolderText={"Please enter a valid object ID or key and press enter"}
                                          value={selectedOption}
                            />

                            {!isPriorVersionSchemaTheSame &&
                                <Alert className={"mt-4"} variant={"warning"}>
                                    <div>
                                        The {selectedTab === "existing" ? "existing" : "suggested"} schema&apos;s names and
                                        types do not match those in the dataset that you have selected to update.
                                        You can not apply this update to this dataset.
                                    </div>
                                </Alert>
                            }
                        </Col>

                        {selectedOption && selectedOption.tag.definition?.data?.schema?.table?.fields &&

                            <Col xs={12} lg={lgGrid} xl={xlGrid}>

                                {/*TODO could show smaller subset*/}
                                <ShowHideDetails linkText={"metadata"}
                                                 classNameOuter={"mt-4 mb-3 pb-3"}>
                                    {/*TODO add button as render props to show full metadata*/}
                                    <MetadataViewer metadata={selectedOption.tag} showTitles={false}
                                                    tablesToShow={[{key: "header"}]}/>
                                </ShowHideDetails>

                            </Col>
                        }
                    </Row>

                </React.Fragment>
            }
        </React.Fragment>
    )
};

export default SetPriorVersion;
