/**
 * A component that shows any details added in the trac-platform.yaml configuration file about the deployment. These are
 * a set of open-ended, user defined, pieces of information
 *
 * @module DeploymentInfoTable
 * @category Component
 */

import {convertKeyToText} from "../utils/utils_string";
import {HeaderTitle} from "./HeaderTitle";
import PropTypes from "prop-types";
import React from "react";
import Table from "react-bootstrap/Table";
import {useAppSelector} from "../../types/types_hooks";

/**
 * An interface for the props of the DeploymentInfoTable component.
 */
export interface Props {

    /**
     * Whether to show a title above the table.
     */
    showTitle: boolean
}

export const DeploymentInfoTable = (props: Props) => {

    const {showTitle} = props;

    // Get what we need from the store
    const {deploymentInfo} = useAppSelector(state => state["applicationStore"].platformInfo)

    return (

        <React.Fragment>

            {deploymentInfo && Object.keys(deploymentInfo).length > 0 &&

                <React.Fragment>
                    {showTitle &&
                        <HeaderTitle type={"h5"} text={"Platform details:"}/>
                    }

                    <Table bordered={true}
                           className={"mt-2 mb-4 table-no-outline"}
                           size={"sm"}
                           striped={true}
                    >
                        <tbody>
                        {Object.entries(deploymentInfo).map(([key, value]) =>

                            <tr key={key}>
                                <td className={"text-nowrap align-top pe-2"}>{convertKeyToText(key)}:</td>
                                <td>
                                    {value}
                                </td>
                            </tr>
                        )}
                        </tbody>
                    </Table>
                </React.Fragment>
            }
        </React.Fragment>
    )
};

DeploymentInfoTable.propTypes = {

    showTitle: PropTypes.bool.isRequired
};