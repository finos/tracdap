/**
 * A component that shows a list of the attributes defined in the dataset and options to view/edit.
 * @module
 * @category Component
 */

import {addRowToEditor, deleteItem} from "../store/applicationSetupStore";
import {Badges} from "../../../components/Badges";
import {Button} from "../../../components/Button";
import {ButtonPayload} from "../../../../types/types_general";
import {ConfigureAttributeModal} from "./ConfigureAttributeModal";
import {ConfirmButton} from "../../../components/ConfirmButton";
import Container from "react-bootstrap/Container";
import {Icon} from "../../../components/Icon";
import React, {useState} from "react";
import Table from "react-bootstrap/Table";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import {ViewAttributeModal} from "./ViewAttributeModal";

export const AttributeListEditor = () => {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {fields} = useAppSelector(state => state["applicationSetupStore"].tracItems.items.ui_attributes_list.currentDefinition)
    const {data} = useAppSelector(state => state["applicationSetupStore"].editor.items.ui_attributes_list)

    // Whether to show the attribute editor modal
    const [show, setShow] = useState(false)

    // State information about the attribute to view in the modal
    const [viewAttributeModel, setViewAttributeModel] = useState<{ show: boolean, attributeId?: string }>({show: false})

    /**
     * A function that runs when the user selects to edit an attribute. This loads the row from the dataset
     * into the editor in the store and opens the modal that allows them to change the values. Not directly editing the
     * dataset means that if the user wants to they can discard their changes.
     *
     * @param payload - The payload from the button click.
     * @param payload.id - The row index in the dataset that the user selected to edit.
     */
    const handleAddRowToEditor = (payload: ButtonPayload): void => {

        const {index} = payload

        if (index != undefined) {
            dispatch(addRowToEditor({id: index}))
            setShow(true)
        }
    }

    return (
        <React.Fragment>

            {data.length === 0 &&
                <Container className={"text-center my-3"}>
                    There are no attributes defined, please use the button below to add one.
                </Container>
            }

            {data.length > 0 &&

                <React.Fragment>

                    <Table responsive className={"dataHtmlTable fs-7 my-3"}>

                        <thead>
                        <tr>
                            {/*Use the labels in the dataset as the column header*/}
                            <th className={"text-nowrap"}>{fields.find(field => field.fieldName === "NAME")?.label}</th>
                            <th className={"text-nowrap"}>{fields.find(field => field.fieldName === "DESCRIPTION")?.label}</th>
                            <th className={"text-center"}>{fields.find(field => field.fieldName === "OBJECT_TYPES")?.label}</th>
                            <th>{""}</th>
                        </tr>
                        </thead>

                        <tbody>

                        {data.map((row, i) =>

                            <React.Fragment key={i}>
                                <tr>
                                    <td className={"py-3 pe-4 align-text-top"}>
                                        {row.NAME || "Not set"}
                                    </td>

                                    <td className={"py-3"}>
                                        {row.DESCRIPTION || "Not set"}
                                    </td>

                                    <td className={"py-3 text-center align-middle"}>
                                        {typeof row.OBJECT_TYPES === "string" ?
                                            <Badges convertText={true}
                                                    delimiter={"||"}
                                                    text={row.OBJECT_TYPES}
                                            />
                                            : "Not set"
                                        }
                                    </td>

                                    <td className={"text-nowrap w-0 align-middle"}>
                                        <Button ariaLabel={`View ${row.ID} attribute`}
                                                className={"me-2 min-width-px-60"}
                                                name={row.ID || undefined}
                                                onClick={(payload: ButtonPayload) => {
                                                    setViewAttributeModel({show: true, attributeId: payload?.name ? payload?.name.toString() : undefined})
                                                }}
                                                isDispatched={false}
                                                variant={"outline-info"}
                                        >
                                            <Icon ariaLabel={false}
                                                  className={"me-2"}
                                                  icon={"bi-binoculars"}
                                            />

                                            View
                                        </Button>

                                        <Button ariaLabel={`Edit ${row.ID} attribute`}
                                                className={"me-3 min-width-px-60"}
                                                index={i}
                                                isDispatched={false}
                                                onClick={handleAddRowToEditor}
                                                variant={"outline-info"}
                                        >
                                            <Icon ariaLabel={false}
                                                  className={"me-2"}
                                                  icon={"bi-pencil"}
                                            />
                                            Edit
                                        </Button>

                                        {/*Don't let the user delete reserved attributes*/}
                                        {!row.RESERVED_FOR_APPLICATION &&
                                            <ConfirmButton ariaLabel={"Delete attribute"}
                                                           className={"m-0 p-0"}
                                                           description={"Are you sure that you want to delete this attribute?"}
                                                           dispatchedOnClick={deleteItem}
                                                           index={i}
                                                           variant={"link"}
                                            >
                                                <Icon icon={"bi-trash3"} ariaLabel={false}/>
                                            </ConfirmButton>
                                        }

                                    </td>
                                </tr>
                            </React.Fragment>
                        )}

                        </tbody>
                    </Table>
                </React.Fragment>
            }

            <ConfigureAttributeModal show={show} toggle={setShow}/>

            <ViewAttributeModal attributeId={viewAttributeModel.attributeId}
                                show={viewAttributeModel.show}
                                toggle={setViewAttributeModel}
            />

        </React.Fragment>
    )
};