import { combineLatest, Observable, of } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { arrayToDataFrame, DataFrame, DataLink, DataLinkConfigOrigin, DataTopic, PanelData } from '@grafana/data';
import { CorrelationData } from '@grafana/data/src/types/panel';

import { DashboardQueryRunnerResult } from './DashboardQueryRunner/types';

function addAnnoDataTopic(annotations: DataFrame[] = []) {
  annotations.forEach((f) => {
    f.meta = {
      ...f.meta,
      dataTopic: DataTopic.Annotations,
    };
  });
}

const decorateDataFrameWithInternalDataLinks = (
  dataFrame: DataFrame,
  correlations: CorrelationData[]
): Array<Array<DataLink<any>>> => {
  let dataFrameFields: Array<Array<DataLink<any>>> = [];
  dataFrame.fields.forEach((field) => {
    let fieldLinks = field.config.links?.filter((link) => link.origin !== DataLinkConfigOrigin.Correlations) || [];
    correlations.forEach((correlation) => {
      if (correlation.config?.field === field.name) {
        const targetQuery = correlation.config?.target || {};
        fieldLinks.push({
          internal: {
            query: { ...targetQuery, datasource: { uid: correlation.target.uid } },
            datasourceUid: correlation.target.uid,
            datasourceName: correlation.target.name,
            transformations: correlation.config?.transformations,
          },
          url: '',
          title: correlation.label || correlation.target.name,
          origin: DataLinkConfigOrigin.Correlations,
        });
      }
    });
    dataFrameFields.push(fieldLinks);
  });
  return dataFrameFields;
};

export function mergePanelAndDashData(
  panelObservable: Observable<PanelData>,
  dashObservable: Observable<DashboardQueryRunnerResult>
): Observable<PanelData> {
  return combineLatest([panelObservable, dashObservable]).pipe(
    mergeMap((combined) => {
      const [panelData, dashData] = combined;

      if (Boolean(dashData.annotations?.length) || Boolean(dashData.alertState) || Boolean(dashData.correlations)) {
        if (!panelData.annotations) {
          panelData.annotations = [];
        }

        const panelDatasources =
          panelData.request === undefined
            ? []
            : panelData.request.targets.map((target) => target.datasource?.uid).filter((uid): uid is string => !!uid);
        const panelCorrelations =
          dashData.correlations === undefined
            ? []
            : dashData.correlations.filter((corr) => panelDatasources?.includes(corr.source.uid));

        panelData.series.forEach((df) => {
          const wat = decorateDataFrameWithInternalDataLinks(df, panelCorrelations);
          console.log('wat', wat);
        });

        const annotations = panelData.annotations.concat(arrayToDataFrame(dashData.annotations));

        addAnnoDataTopic(annotations);

        const alertState = dashData.alertState;
        return of({ ...panelData, annotations, alertState, correlations: dashData.correlations });
      }

      addAnnoDataTopic(panelData.annotations);

      return of(panelData);
    })
  );
}
