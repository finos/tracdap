/**
 * A component that shows a row in the table in the {@link SelectBatchDatasets} component. It is memoized to reduce re-renders.
 * @module BatchDatasetRow
 * @category BatchDataImportScene component
 */

import {addOrRemoveImportDataFromBatch, setDate} from "../store/batchImportDataStore";
import {Button} from "../../../components/Button";
import {ButtonPayload, UiBatchImportDataRow} from "../../../../types/types_general";
import format from "date-fns/format";
import {Icon} from "../../../components/Icon";
import {isDateFormat} from "../../../utils/utils_trac_type_chckers";
import parseISO from "date-fns/parseISO";
import {SelectDate} from "../../../components/SelectDate";
import {SelectToggle} from "../../../components/SelectToggle";
import React, {memo} from "react";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the BatchDatasetRow component.
 */
export interface Props {

    /**
     * The date value that the user has set for the import, this is used to identify the right data to import.
     */
    date: null | string
    /**
     * Whether the user has selected to load the dataset.
     */
    load: boolean
    /**
     * A row from the batch data import definitions, these are owned and edited in the {@link ApplicationSetupScene}.
     */
    row: UiBatchImportDataRow
    /**
     * A function that toggles whether to show the modal with the full record a batch data import option.
     */
    toggleModal: (payload: ButtonPayload) => void
    /**
     * Whether the user has tried to submit a batch job and whether the validation information is shown.
     */
    validationChecked: boolean
}

const BatchDatasetRow = (props: Props) => {

    const {
        date,
        load,
        row,
        toggleModal,
        validationChecked
    } = props

    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    return (
        <tr className={"py-1"} key={row.DATASET_ID}>

            <td className={"text-nowrap py-3 pe-4 align-text-top"}>
                {row.DATASET_NAME || "Not set"}
            </td>

            <td className={"py-3 d-none d-lg-table-cell"}>
                {row.DATASET_DESCRIPTION || "Not set"}
            </td>

            <td className={"py-3 d-none d-md-table-cell"}>
                {row.DATASET_SOURCE_SYSTEM || "Not set"}
            </td>

            {/*If there is no need for a widget then make the text the same vertical position as the others*/}
            <td className={`align-middle ${row.DATASET_FREQUENCY ? "" : "py-3"}`}>
                {row.DATASET_FREQUENCY ?
                    <SelectDate basicType={trac.DATE}
                        // Turn off the invalid icon as it causes a jump in the UI when shown
                                className={"no-invalid-icon"}
                                formatCode={isDateFormat(row.DATASET_FREQUENCY) ? row.DATASET_FREQUENCY : null}
                                helperText={date !== null && row.DATASET_DATE_REGEX !== null ? `Dataset name: '${format(parseISO(date), row.DATASET_DATE_REGEX)}'` : undefined}
                        // Disable also if the user is in time travel mode
                                isDisabled={Boolean(row.DISABLED || searchAsOf != null)}
                                isDispatched={true}
                                name={row.DATASET_ID}
                                onChange={setDate}
                                placeHolderText={row.DISABLED ? "Disabled" : undefined}
                                mustValidate={true}
                                showValidationMessage={false}
                        // Default is off so no need to validate on mount
                                validateOnMount={false}
                                validationChecked={validationChecked}
                                value={date}
                    />
                    : "Not needed"}
            </td>

            <td className={"text-center align-middle"}>
                <Button ariaLabel={`View ${row.DATASET_ID} details`}
                        className={"m-0 p-0"}
                        isDispatched={false}
                        name={row.DATASET_ID || undefined}
                        onClick={toggleModal}
                        variant={"link"}
                >
                    <Icon ariaLabel={false}
                          className={"me-2"}
                          icon={"bi-list-ul"}
                          size={"1.5rem"}
                    />
                </Button>

            </td>

            <td className={"text-center mx-2 align-middle"}>

                {/*We put the data frequency in the toggle so when we toggle we know*/}
                {/*whether a null date is valid*/}
                <SelectToggle id={row.DATASET_FREQUENCY || undefined}
                              isDisabled={row.DISABLED}
                              isDispatched={true}
                              mustValidate={false}
                              name={row.DATASET_ID || "dummy"}
                              onChange={addOrRemoveImportDataFromBatch}
                              showValidationMessage={false}
                              validateOnMount={false}
                              value={load}
                />

            </td>
        </tr>
    )
};

export default memo(BatchDatasetRow)
