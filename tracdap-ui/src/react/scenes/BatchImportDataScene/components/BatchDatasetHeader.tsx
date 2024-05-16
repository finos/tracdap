/**
 * A component that shows a header in the table in the {@link SelectBatchDatasets} component. It is memoized to reduce re-renders.
 * @module BatchDatasetHeader
 * @category BatchDataImportScene component
 */

import {Icon} from "../../../components/Icon";
import React, {memo} from "react";

const BatchDatasetHeader = () => (

    <thead>
    <tr>
        <th className={"text-nowrap"}>
            Name
        </th>
        <th className={"text-nowrap d-none d-lg-table-cell"}>
            Description
        </th>
        <th className={"text-nowrap d-none d-md-table-cell"}>
            Source system
        </th>
        <th className={"text-nowrap text-center"}>
            Date data is for <Icon ariaLabel={"More information"}
                                   className={"ms-2"}
                                   icon={"bi-question-circle"}
                                   tooltip={"Some datasets have dates in their names to identify that they are for a particular month or day. Select the date that you need below, the string that will be searched for in the dataset's name will be shown."}
        />
        </th>
        <th className={"text-nowrap text-center"}>
            View full record
        </th>
        <th className={"text-center text-nowrap"}>
            Load
        </th>
    </tr>
    </thead>
)

export default memo(BatchDatasetHeader)