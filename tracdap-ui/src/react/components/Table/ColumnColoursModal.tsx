/**
 * A component that shows a modal that allows the user to set which columns in a table have heat maps or
 * traffic lights and what the colours are.
 *
 * @module ColumnColoursModal
 * @category Component
 */

// TODO review his component
import {Button} from "../Button";
import Col from "react-bootstrap/Col";
import {convertHexToRgb, getColumnColourTransitionValue, getYiq} from "../../utils/utils_general";
import {convertKeyToText} from "../../utils/utils_string";
import type {ColumnColourDefinition, ColumnColourDefinitions, Option, SelectOptionPayload, TableRow, TracUiTableState} from "../../../types/types_general";
import {getMinAndMaxValueFromArrayOfObjects} from "../../utils/utils_arrays";
import {hasOwnProperty, isColumnColourType, isDateObject, isTracBoolean, isTracDateOrDatetime, isTracNumber} from "../../utils/utils_trac_type_chckers";
import Modal from "react-bootstrap/Modal";
import PropTypes from "prop-types";
import React from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../SelectOption";
import type {Table} from "@tanstack/react-table";
import {TableColumnColours} from "../../../config/config_general";
import {tracdap as trac} from "@finos/tracdap-web-api";

// Some Bootstrap grid layouts
const lgGrid = {span: 8, offset: 2};
const mdGrid = {span: 10, offset: 1};
const xsGrid = {span: 12, offset: 0};

// The options for the column colouring
const options = [{value: "none", label: "None"},
    {value: "heatmap", label: "Heatmap"},
    {value: "trafficlight", label: "Traffic light"}]

/**
 * An interface for the props of the ColumnColoursModal component.
 */
export interface Props {

    /**
     * The data for the table, used to calculate the minimum and maximum values for the heatmap range.
     */
    data: TableRow[]
    /**
     * The heatmap defining which ones have heatmaps set and what the colours are and what the minimum and maximum
     * bounds of the heatmap range are.
     */
    columnColours: ColumnColourDefinitions
    /**
     * The table object from the react-table plugin.
     */
    table: Table<TableRow>
    /**
     * A function that toggles whether the modal is shown or not.
     */
    toggle: React.Dispatch<React.SetStateAction<boolean>>
    /**
     * A function that updates the user defined heatmap.
     */
    setTracUiState: React.Dispatch<React.SetStateAction<TracUiTableState>>
    /**
     * Whether to show the modal or not.
     */
    show: boolean
}

export const ColumnColoursModal = (props: Props) => {

    const {
        columnColours,
        data,
        setTracUiState,
        show,
        table,
        toggle,
    } = props

    /**
     * A function that runs when the user changes the type of colour map to use for a column.
     * @param payload - The payload from the {@link SelectOption} component.
     */
    const onChangeType = (payload: SelectOptionPayload<Option<string>, false>) => {

        const {basicType, id, value: option} = payload

        const value = option ? option.value : undefined

        // Some type protecting errors
        if (typeof id !== "string") throw new TypeError("The heat map option did not hava an ID")
        if (!isColumnColourType(value)) throw new TypeError("The heat map type is not valid")

        setTracUiState((prevState) => {

            let newState: ColumnColourDefinition

            // If the column already has some colour settings then edit those
            if (hasOwnProperty(prevState, id)) {

                newState = {...prevState.columnColours[id]}
                // Use the checkbox value to set whether heat map is on or off
                newState.type = value

            } else {

                // Set the colour based on the colour mapping method
                const lowColour = value === "heatmap" ? TableColumnColours.lowHeatmapColour : TableColumnColours.lowTrafficLightColour
                const highColour = value === "heatmap" ? TableColumnColours.highHeatmapColour : TableColumnColours.highTrafficLightColour

                // Get the hexadecimal colours as rgb
                const lowColourAsRgb = convertHexToRgb(lowColour)
                const highColourAsRgb = convertHexToRgb(highColour)
                const transitionColourAsRgb = convertHexToRgb(TableColumnColours.transitionTrafficLightColour)

                // Otherwise, add in a new entry
                newState = {
                    basicType,
                    highColour,
                    lowColour,
                    maximumValue: undefined,
                    minimumValue: undefined,
                    transitionColour: TableColumnColours.transitionTrafficLightColour,
                    // The default colour between the high and low is a null value for numbers and dates we
                    // later have a guess at an initial value but for booleans it will be null
                    transitionValue: null,
                    type: value,
                    // Calculate the colours for the text, using YIQ to decide which will give the higher contrast
                    lowTextColour: value === "trafficlight" ? getYiq(lowColourAsRgb.r, lowColourAsRgb.g, lowColourAsRgb.b) >= 128 ? 'black' : 'white' : undefined,
                    highTextColour: value === "trafficlight" ? getYiq(highColourAsRgb.r, highColourAsRgb.g, highColourAsRgb.b) >= 128 ? 'black' : 'white' : undefined,
                    transitionTextColour: getYiq(transitionColourAsRgb.r, transitionColourAsRgb.g, transitionColourAsRgb.b) >= 128 ? 'black' : 'white'
                }
            }

            // Check if we need to calculate the minimum and maximum values for the heat map, this is expensive, so we
            // always retain the calculated values in the colour definition state, this is so if the user turns it on
            // then off and then on again then we don't have to recalculate the min and max. Instead the values from the
            // first calculation will be available.
            if (newState.minimumValue === undefined || newState.maximumValue === undefined) {

                const {minimum, maximum} = getMinAndMaxValueFromArrayOfObjects(data, id)
                newState.minimumValue = isDateObject(minimum) ? undefined : minimum
                newState.maximumValue = isDateObject(maximum) ? undefined : maximum
                // Traffic lights also need a value to use as the transition value
                newState.transitionValue = getColumnColourTransitionValue(newState.minimumValue, newState.maximumValue, basicType)
            }

            // Copy the new parameters to the store
            return {...prevState, ...{columnColours: {[id]: newState}}}
        })
    }

    return (

        <Modal show={show} onHide={() => toggle(false)}>

            <Modal.Header closeButton>
                <Modal.Title>
                    Set which columns have a heat map
                </Modal.Title>
            </Modal.Header>

            <Modal.Body className={"mx-5"}>

                <Row>
                    <Col xs={xsGrid} md={mdGrid} lg={lgGrid}>

                        {table.getVisibleLeafColumns().filter(column => isTracDateOrDatetime(column.columnDef.meta?.schema.fieldType) || isTracNumber(column.columnDef.meta?.schema.fieldType) || isTracBoolean(column.columnDef.meta?.schema.fieldType)).map(column => {
                            return (
                                <div key={column.id}>
                                    {`${column.columnDef.header}`}
                                    <SelectOption basicType={column.columnDef.meta?.schema.fieldType || trac.BasicType.BASIC_TYPE_NOT_SET}
                                                  id={column.id}
                                                  isDispatched={false}
                                                  mustValidate={false}
                                                  onChange={onChangeType}
                                                  options={options}
                                                  validateOnMount={false}
                                                  value={{
                                                      value: columnColours[column.id]?.type || "none",
                                                      label: convertKeyToText(columnColours[column.id]?.type || "None")
                                                  }}
                                    />
                                </div>
                            )
                        })}

                        {table.getVisibleLeafColumns().length === 0 &&
                            "There are no visible number, boolean or date columns eligible to have heat maps applied."
                        }
                    </Col>
                </Row>
            </Modal.Body>

            <Modal.Footer>

                <Button ariaLabel={"Close heatmap menu"}
                        variant={"secondary"}
                        onClick={() => toggle(false)}
                        isDispatched={false}
                >
                    Close
                </Button>

            </Modal.Footer>
        </Modal>
    )
};

ColumnColoursModal.propTypes = {

    columnColours: PropTypes.object.isRequired,
    data: PropTypes.array.isRequired,
    setTracUiState: PropTypes.func.isRequired,
    show: PropTypes.bool.isRequired,
    toggle: PropTypes.func.isRequired,
    table: PropTypes.object.isRequired,
};