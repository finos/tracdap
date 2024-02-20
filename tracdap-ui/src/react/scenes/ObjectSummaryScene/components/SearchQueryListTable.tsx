import {Icon} from "../../../components/Icon";
import PropTypes from "prop-types";
import React from "react";
import Table from "react-bootstrap/Table";

/**
 * A component that shows a table listing the queries used by a flow to find the input datasets or models for a flow.
 * This component is for viewing in a browser, there is a sister component called {@link SearchQueryListPdf} that is
 * for viewing in a PDF. These two components need to be kept in sync so if a change is made to one then it should be
 * reflected in the other.
 */

type Props = {

    /**
     * The css class to apply to the alert, this allows additional styles to be added to the component.
     */
    className: string
    /**
     * The queries used to find the list of options for the object in TRAC. It is undefined if no nodeSearch property
     * exists for the object in the flow, in which case this component will say that the key value is used - which is
     * the default behaviour of this application.
     */
    queries: { [key: string]: undefined | { attrName: string, value: string } }
};

const SearchQueryListTable = (props: Props) => {

    const {className, queries} = props

    return (

        <React.Fragment>

            {(!queries || Object.keys(queries).length === 0) &&
                <div className={"mb-4"}>There are no search queries</div>
            }

            {queries && Object.keys(queries).length > 0 &&

                <Table className={`dataHtmlTable w-100 ${className}`}>
                    <thead>
                    <tr>

                        <th>
                            Attribute<Icon ariaLabel={"Info"}
                                            className={"ms-2"}
                                            icon={"bi-info-circle"}
                                            tooltip={"The attribute that will be searched"}/>
                        </th>

                        <th>
                            Value<Icon ariaLabel={"Info"}
                                        className={"ms-2"}
                                        icon={"bi-info-circle"}
                                        tooltip={"The value that will be searched for"}/>
                        </th>

                    </tr>
                    </thead>
                    <tbody>
                    {Object.entries(queries).map(([key, queryObject]) => (
                        <tr key={key}>

                            <td>
                                {queryObject ? queryObject.attrName : "key"}
                            </td>

                            <td>
                                {queryObject ? queryObject.value : key}
                            </td>
                        </tr>)
                    )}
                    </tbody>
                </Table>
            }
        </React.Fragment>
    )
};

SearchQueryListTable.propTypes = {

    className: PropTypes.string,
    queries: PropTypes.object
};

SearchQueryListTable.defaultProps = {

    className: ""
};

export default SearchQueryListTable;