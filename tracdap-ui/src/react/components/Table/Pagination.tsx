/**
 * A component showing pagination widgets for navigating a paginated table.
 *
 * @module Pagination
 * @category Component
 */

import BSPagination from "react-bootstrap/Pagination";
import {Icon} from "../Icon";
import PropTypes from "prop-types";
import React, {memo} from "react";

/**
 * An interface for the props of the Pagination component.
 */
export interface Props {

    /**
     * Whether the current page has a next page.
     */
    canNextPage: boolean
    /**
     * Whether the current page has a previous page.
     */
    canPreviousPage: boolean
    /**
     * The css class to apply to the buttons, this allows additional styles to be added to the component.
     */
    className: string
    /**
     * A function that navigates to the next page.
     */
    nextPage: () => void
    /**
     * The total number of pages.
     */
    pageCount: number
    /**
     * A function that navigates to the previous page.
     */
    previousPage: () => void
    /**
     * A function that navigates to the page corresponding to the provided index..
     */
    setPageIndex: (index: number) => void
}

const PaginationInner = (props: Props) => {

    const {
        canNextPage,
        canPreviousPage,
        className,
        nextPage,
        pageCount,
        previousPage,
        setPageIndex
    } = props

    return (

        <BSPagination className={`${className} mb-0`}>
            <BSPagination.Item onClick={() => setPageIndex(0)}
                               disabled={!canPreviousPage}
            >
                <Icon icon={"bi-chevron-double-left"} ariaLabel={"first page"}/>
            </BSPagination.Item>
            <BSPagination.Item onClick={() => previousPage()}
                               disabled={!canPreviousPage}
            >
                <Icon icon={"bi-chevron-left"} ariaLabel={"previous page"}/>
            </BSPagination.Item>

            <BSPagination.Item onClick={() => nextPage()}
                               disabled={!canNextPage}
            >
                <Icon icon={"bi-chevron-right"} ariaLabel={"next page"}/>
            </BSPagination.Item>
            <BSPagination.Item onClick={() => setPageIndex(pageCount - 1)}
                               disabled={!canNextPage}
            >
                <Icon icon={"bi-chevron-double-right"} ariaLabel={"last page"}/>
            </BSPagination.Item>
        </BSPagination>
    )
};

PaginationInner.propTypes = {

    className: PropTypes.string,
    canNextPage: PropTypes.bool.isRequired,
    canPreviousPage: PropTypes.bool.isRequired,
    nextPage: PropTypes.func.isRequired,
    pageCount: PropTypes.number.isRequired,
    previousPage: PropTypes.func.isRequired,
    setPageIndex: PropTypes.func.isRequired
};

export const Pagination = memo(PaginationInner);