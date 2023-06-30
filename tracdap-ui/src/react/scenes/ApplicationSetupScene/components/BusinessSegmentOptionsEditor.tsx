/**
 * A component that shows a table that allows the user to pick a business segment to edit.
 * @module
 * @category Component
 */

import {addRowToData, addRowToEditor, deleteItem, editBusinessSegmentTitle} from "../store/applicationSetupStore";
import {Button} from "../../../components/Button";
import Col from "react-bootstrap/Col";
import {ConfigureBusinessSegmentModal} from "./ConfigureBusinessSegmentModal";
import {ConfirmButton} from "../../../components/ConfirmButton";
import {getFieldLabelFromSchema} from "../../../utils/utils_schema";
import {Icon} from "../../../components/Icon";
import {NumberBubble} from "../../../components/NumberBubble";
import React, {useState} from "react";
import Row from "react-bootstrap/Row";
import {SelectValue} from "../../../components/SelectValue";
import {TextBlock} from "../../../components/TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {isKeyOf} from "../../../utils/utils_trac_type_chckers";

export const BusinessSegmentOptionsEditor = () => {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {
        data: editedData,
        fields,
        validation: {isValid, validationChecked}
    } = useAppSelector(state => state["applicationSetupStore"].editor.items.ui_business_segment_options)

    const [show, setShow] = useState<boolean>(false)

    /**
     * A function that runs when the user selects to edit a business segment option. This loads the row from the dataset
     * into the editor in the store and opens the modal that allows them to change the values. Not directly editing the
     * dataset means that if the user wants to they can discard their changes. What we are trying to do here is to allow
     * null labels to be set, they will fail validation, but it is a valid entry for the input.
     *
     * @param payload - The payload from the button click.
     * @param payload.id - The row index in the dataset that the user selected to edit.
     */
    const handleAddRowToEditor = (payload: { id: number }): void => {

        dispatch(addRowToEditor(payload))
        setShow(true)
    }

    return (
        <React.Fragment>

            {editedData.length > 0 &&

                <React.Fragment>

                    <TextBlock>
                        The business segment dataset represents a hierarchy of up to four levels or groups that will be presented
                        to the user to allow them to tag their models, data, etc. with so that they can be catalogued and searched
                        for. This means that if a user selects a level 1 option when searching for a job (for example) then they
                        will be presented with a set of linked level 2 options to select from. You do not have to set all four
                        levels of options. You can for example just set level 1 options if you need to.
                    </TextBlock>

                    <HeaderTitle type={"h4"} text={"Labels for each group:"}/>

                    <Row className={"pt-4 px-3 gx-1 background-secondary"}>

                        {[1, 2, 3, 4].map(level => (
                            <Col xs={12} md={6} lg={3} className={"label"} key={level}>
                                <SelectValue basicType={trac.STRING}
                                             className={"mx-1 mb-1 bg-input-body-background"}
                                             id={`GROUP_0${level}_NAME`}
                                             labelText={`Group ${level} label`}
                                             labelPosition={"top"}
                                             mustValidate={true}
                                             onChange={editBusinessSegmentTitle}
                                             showValidationMessage={true}
                                             validationChecked={validationChecked}
                                             validateOnMount={true}
                                             value={getFieldLabelFromSchema(fields, `GROUP_0${level}_NAME`, `Level ${level}`)}
                                />
                            </Col>
                        ))}

                    </Row>

                    <HeaderTitle type={"h4"} text={"Options for each group:"} tooltip={"Click on the pencil icon to edit the business segment."}/>

                    {editedData.map((row, i) => (

                        <Row className={"mt-3 px-3 py-3 gx-1 background-secondary"} key={i}>

                            <Col xs={"auto"} className={"my-auto me-2"}>
                                <NumberBubble text={i + 1}/>
                            </Col>

                            <Col>
                                <Row className={"gx-0"}>
                                {[1, 2, 3, 4].map(level => {

                                    const variable_id = `GROUP_0${level}_ID`
                                    const variable_name = `GROUP_0${level}_NAME`
                                    const isSet = isKeyOf(row, variable_id) && isKeyOf(row, variable_name) && row[variable_name] !== null

                                    return (
                                        <Col xs={12} md={6} lg={6} xxxl={3} className={`align-items-center py-2 d-flex px-xxxl-2 ${[1,3].includes(level) ? "pe-md-2": "ps-md-2"} ${[1,2].includes(level) ? "pb-md-2": "pt-md-2"}`} key={level}>
                                            <div className={`flex-fill pb-1 border-bottom ${!isSet ? "label": ""} ${validationChecked && isValid[i] && isValid[i][`GROUP_0${level}`] === false ? "border-danger": ""}`}>
                                                {isSet ? `${row[variable_name]} (${row[variable_id]})` : `Group ${level} not set`}
                                            </div>
                                            <Button ariaLabel={`Edit business segment`}
                                                    className={"m-0 p-0 ms-2 flex-shrink-0"}
                                                    id={i}
                                                    name={`GROUP_0${level}`}
                                                    isDispatched={false}
                                                    onClick={handleAddRowToEditor}
                                                    variant={"link"}
                                            >
                                                <Icon icon={"bi-pencil"} ariaLabel={false}
                                                      tooltip={`Edit level ${level}`}/>
                                            </Button>
                                        </Col>
                                    )
                                })}
                                    </Row>
                            </Col>
                            <Col xs={"auto"} className={"my-auto ms-3"}>

                                <ConfirmButton ariaLabel={"Delete row"}
                                               className={"m-0 p-0 me-2"}
                                               description={"Are you sure that you want to delete this business segment?"}
                                               dispatchedOnClick={deleteItem}
                                               index={i}
                                               variant={"link"}
                                >
                                    <Icon icon={"bi-trash3"} ariaLabel={false}/>
                                </ConfirmButton>

                                <Button ariaLabel={"Copy row"}
                                        className={"m-0 p-0"}
                                        index={i}
                                        name={"copy"}
                                        onClick={addRowToData}
                                        variant={"link"}
                                >
                                    <Icon ariaLabel={false} icon={"bi-files"}/>
                                </Button>

                            </Col>

                        </Row>
                    ))}

                </React.Fragment>
            }

            <ConfigureBusinessSegmentModal toggle={setShow} show={show}/>

        </React.Fragment>
    )
};