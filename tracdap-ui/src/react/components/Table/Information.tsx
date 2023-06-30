/**
 * A component showing information about the table data and what they have selected.
 *
 * @module Information
 * @category Component
 */

import {isOrAre, sOrNot} from "../../utils/utils_general";
import PropTypes from "prop-types";
import React, {memo} from "react";

/**
 * An interface for the props of the Information component.
 */
export interface Props {

    /**
     * The css class to apply to the information, this allows additional styles to be added to the component.
     * @defaultValue ''
     */
    className?: string
    /**
     * The number of results after global filtering is applied.
     */
    filteredLength: number
    /**
     * The total number of pages.
     */
    pageCount?: number
    /**
     * The current page index.
     */
    pageIndex?: number
    /**
     * The number of rows per page when paginated.
     */
    pageSize: number
    /**
     * Whether the table is paginated or not.
     */
    paginate: boolean
    /**
     * The number of rows in the table when un-paginated.
     */
    rowsLength: number
    /**
     * The number of selected rows in the table that are in the filtered results.
     */
    selectedAndFilteredLength: number
    /**
     * The number of selected rows in the table.
     */
    selectedLength: number
}

const InformationInner = (props: Props) => {

    const {
        className = "",
        filteredLength,
        pageCount,
        pageIndex,
        pageSize,
        paginate,
        rowsLength,
        selectedAndFilteredLength,
        selectedLength
    } = props

    // Handle plurals in the number of rows
    const sOrNot1 = sOrNot(selectedLength)
    const sOrNot2 = sOrNot(filteredLength)
    const sOrNot3 = sOrNot(selectedLength - selectedAndFilteredLength)

    // Work out the text to show based on the options
    const text = () => {

        let text

        const selectedButFilteredText = Boolean(selectedLength - selectedAndFilteredLength > 0) ? ` (${selectedLength - selectedAndFilteredLength} selected row${sOrNot3} ${isOrAre(selectedLength - selectedAndFilteredLength)} being filtered out)` : ""

        if ((!paginate || (pageSize != null && rowsLength <= pageSize))) {

            if (filteredLength === rowsLength) {
                text = `Showing ${rowsLength} row${sOrNot2}`
            } else {
                text = `Showing ${filteredLength} from ${rowsLength} rows`
            }

            if (selectedLength > 0) {
                text = `${text}, selected ${selectedLength} row${sOrNot1}${selectedButFilteredText}`
            }

        } else if (pageIndex != null) {

            if (selectedLength === 0 || !selectedLength) {
                text = `Showing page ${pageCount === 0 ? 0 : pageIndex + 1} of ${pageCount}`
            } else {
                text = `Selected ${selectedLength}${selectedButFilteredText} row${sOrNot1}, showing page ${pageIndex + 1} of ${pageCount}`
            }

            if (filteredLength && filteredLength === rowsLength) {
                text = `${text} (${rowsLength} row${sOrNot2})`
            } else if (filteredLength && filteredLength !== rowsLength) {
                text = `${text} (${filteredLength} row${sOrNot2} filtered from ${rowsLength})`
            }
        }

        return text
    }

    return (

        <span className={className}>
            {text()}
        </span>
    )
};

InformationInner.propTypes = {

    className: PropTypes.string,
    filteredLength: PropTypes.number.isRequired,
    pageCount: PropTypes.number,
    pageIndex: PropTypes.number,
    pageSize: PropTypes.number.isRequired,
    paginate: PropTypes.bool.isRequired,
    rowsLength: PropTypes.number.isRequired,
    selectedAndFilteredLength: PropTypes.number.isRequired,
    selectedLength: PropTypes.number.isRequired
};

export const Information = memo(InformationInner);