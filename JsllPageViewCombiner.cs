using Newtonsoft.Json;
using JsllDataProcessHelper.Models;
using ScopeRuntime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace JsllDataProcessHelper
{
    public class JsllPageViewCombiner : Combiner
    {
        int MaxEventCount = 500;
        public override Schema Produces(string[] requestedColumns, string[] args, Schema leftSchema, string leftTable, Schema rightSchema, string rightTable)
        {
            Schema schema = new Schema();
            // Identifiers
            schema.Add(new ColumnInfo("Anid", ColumnDataType.String) { Source = leftSchema[leftSchema["Anid"]] });
            schema.Add(new ColumnInfo("ContentId", ColumnDataType.String) { Source = leftSchema[leftSchema["ContentId"]] });
            schema.Add(new ColumnInfo("head_event_id", ColumnDataType.String));
            schema.Add(new ColumnInfo("MUID", ColumnDataType.String) { Source = leftSchema[leftSchema["MUID"]] });
            schema.Add(new ColumnInfo("PageViewId", ColumnDataType.String) { Source = leftSchema[leftSchema["PageViewId"]] });
            schema.Add(new ColumnInfo("ProcessDateTime", ColumnDataType.DateTime));
            schema.Add(new ColumnInfo("puidhash", ColumnDataType.String) { Source = leftSchema[leftSchema["puidhash"]] });
            schema.Add(new ColumnInfo("TopicKey", ColumnDataType.String) { Source = leftSchema[leftSchema["TopicKey"]] });
            schema.Add(new ColumnInfo("VisitorId", ColumnDataType.String) { Source = leftSchema[leftSchema["mc1_visitor_id"]] });

            // HeadEvent Data
            schema.Add(new ColumnInfo("Author", ColumnDataType.String));
            schema.Add(new ColumnInfo("Contentlang", ColumnDataType.String));
            schema.Add(new ColumnInfo("cpnid", ColumnDataType.String));

            schema.Add(new ColumnInfo("custom_tag_list", ColumnDataType.String) { Source = leftSchema[leftSchema["custom_tag_list"]] });
            schema.Add(new ColumnInfo("enrich_session_id", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_session_id"]] });
            schema.Add(new ColumnInfo("enrich_rip_isp", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_isp"]] });
            schema.Add(new ColumnInfo("enrich_url_pg_query_string", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_url_pg_query_string"]] });

            schema.Add(new ColumnInfo("IsMSInternalTraffic", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("LastReviewed", ColumnDataType.DateTime));
            schema.Add(new ColumnInfo("Moniker", ColumnDataType.String));
            schema.Add(new ColumnInfo("MSProd", ColumnDataType.String));
            schema.Add(new ColumnInfo("MSService", ColumnDataType.String));

            schema.Add(new ColumnInfo("Locale", ColumnDataType.String) { Source = leftSchema[leftSchema["CustomTagLocale"]] });
            schema.Add(new ColumnInfo("ReferrerDomain", ColumnDataType.String) { Source = leftSchema[leftSchema["referrer_domain"]] });
            schema.Add(new ColumnInfo("ReferrerName", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_referrer_name"]] });
            schema.Add(new ColumnInfo("ReferrerSearchPhrase", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_referrer_search_phrase"]] });
            schema.Add(new ColumnInfo("ReferrerType", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_referrer_type"]] });
            schema.Add(new ColumnInfo("ReferrerQueryString", ColumnDataType.String) { Source = leftSchema[leftSchema["referrer_query_string"]] });
            schema.Add(new ColumnInfo("Referrer_uri_stem", ColumnDataType.String) { Source = leftSchema[leftSchema["referrer_uri_stem"]] });

            schema.Add(new ColumnInfo("ProductFamilyName", ColumnDataType.String) { Source = leftSchema[leftSchema["ProductFamilyName"]] });
            schema.Add(new ColumnInfo("ProductVersion", ColumnDataType.Integer) { Source = leftSchema[leftSchema["ProductVersion"]] });
            schema.Add(new ColumnInfo("Site", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_url_pg_domain"]] });
            schema.Add(new ColumnInfo("StartDateTime", ColumnDataType.DateTime) { Source = leftSchema[leftSchema["server_utc_datetime"]] });

            schema.Add(new ColumnInfo("target_url_events", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("target_url_Json", ColumnDataType.String));

            schema.Add(new ColumnInfo("Title", ColumnDataType.String) { Source = leftSchema[leftSchema["pg_title"]] });
            schema.Add(new ColumnInfo("Theme_First", ColumnDataType.String));
            schema.Add(new ColumnInfo("Theme_Last", ColumnDataType.String));
            schema.Add(new ColumnInfo("TopicType", ColumnDataType.String));
            schema.Add(new ColumnInfo("Url", ColumnDataType.String));

            schema.Add(new ColumnInfo("UserBrowser", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_ua_browser_name"]] });
            schema.Add(new ColumnInfo("UserBrowserLang", ColumnDataType.String) { Source = leftSchema[leftSchema["browser_lang"]] });
            schema.Add(new ColumnInfo("UserClientIP", ColumnDataType.String) { Source = leftSchema[leftSchema["client_ip"]] });
            schema.Add(new ColumnInfo("UserDevice", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_ua_device"]] });
            schema.Add(new ColumnInfo("UserOS", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_ua_os"]] });
            schema.Add(new ColumnInfo("UserContinent", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_continent"]] });
            schema.Add(new ColumnInfo("UserCountry", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_country"]] });
            schema.Add(new ColumnInfo("UserRegion", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_region"]] });
            schema.Add(new ColumnInfo("UserState", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_state"]] });
            schema.Add(new ColumnInfo("UserCity", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_city"]] });
            schema.Add(new ColumnInfo("UserLatitude", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_latitude"]] });
            schema.Add(new ColumnInfo("UserLongitude", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_longitude"]] });
            schema.Add(new ColumnInfo("enrich_rip_organization", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_organization"]] });
            schema.Add(new ColumnInfo("enrich_rip_industry", ColumnDataType.String) { Source = leftSchema[leftSchema["enrich_rip_industry"]] });
            schema.Add(new ColumnInfo("enrich_is_bot", ColumnDataType.BooleanQ) { Source = leftSchema[leftSchema["enrich_is_bot"]] });
            //JSLL event
            schema.Add(new ColumnInfo("CustomEvent", ColumnDataType.String) { Source = leftSchema[leftSchema["CustomEvent"]] });
            schema.Add(new ColumnInfo("ActionType", ColumnDataType.String) { Source = leftSchema[leftSchema["ActionType"]] });
            schema.Add(new ColumnInfo("VScroll", ColumnDataType.LongQ) { Source = leftSchema[leftSchema["VScroll"]] });
            schema.Add(new ColumnInfo("HScroll", ColumnDataType.LongQ) { Source = leftSchema[leftSchema["HScroll"]] });
            schema.Add(new ColumnInfo("PageHeight", ColumnDataType.LongQ) { Source = leftSchema[leftSchema["PageHeight"]] });
            schema.Add(new ColumnInfo("Behavior", ColumnDataType.IntegerQ) { Source = leftSchema[leftSchema["Behavior"]] });
            schema.Add(new ColumnInfo("IsUserConsent", ColumnDataType.String) { Source = leftSchema[leftSchema["IsUserConsent"]] });
            schema.Add(new ColumnInfo("ImpressionGuid", ColumnDataType.String) { Source = leftSchema[leftSchema["ImpressionGuid"]] });
            

            // Calcualated Properties 
            schema.Add(new ColumnInfo("TabbedEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("IsTabbedEvents", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("AzureCliLogninEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("ActivePageViewLength", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("APISearchEvents", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("APISearchEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("APISearchLastMoniker", ColumnDataType.String));
            schema.Add(new ColumnInfo("APISearchLastResults", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("APISearchLastTerm", ColumnDataType.String));
            schema.Add(new ColumnInfo("CloudShellEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("CopyEvents", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("CopyEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("EventCount", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("ExperimentEventJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("IsBounce", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedAPISearch", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedComments", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedCloudShell", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedCopyButton", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedDownloadPDF", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedEdit", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedFreeAccount", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedLocaleSearch", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedMoniker", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedShare", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedSiteSearch", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedTheme", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsClickedTOC", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsExperiment", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsLocFallback", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("LinkClickEvents", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("LinkClickEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("LocaleSearchEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("MaxScroll", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("PageLoadTime", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("PageViewLength", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("PerfTimingEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("ScrollEvents", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("ScrollEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("SiteSearchEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("SwitcherEvents", ColumnDataType.Integer));
            schema.Add(new ColumnInfo("SwitcherEventsJson", ColumnDataType.String));

            //Ch9
            schema.Add(new ColumnInfo("Ch9Environment", ColumnDataType.String));
            schema.Add(new ColumnInfo("Ch9PageType", ColumnDataType.String));
            schema.Add(new ColumnInfo("Ch9Partnerid", ColumnDataType.String));
            schema.Add(new ColumnInfo("Ch9VideoEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("Ch9VideoCompletionRate", ColumnDataType.Double));
            schema.Add(new ColumnInfo("Ch9VideoLength", ColumnDataType.String));
            schema.Add(new ColumnInfo("Ch9VideoWatchtime", ColumnDataType.String));
            schema.Add(new ColumnInfo("Ch9Asst",ColumnDataType.String));
            schema.Add(new ColumnInfo("Ch9VideoPlayDateTime", ColumnDataType.DateTimeQ));
            schema.Add(new ColumnInfo("Ch9VideoPlayDelay", ColumnDataType.IntegerQ));
            schema.Add(new ColumnInfo("Ch9SessionTags", ColumnDataType.String));
            schema.Add(new ColumnInfo("Ch9StreamUrl", ColumnDataType.String));//placeholder
            schema.Add(new ColumnInfo("Ch9ErrorList", ColumnDataType.String));//placeholder
            schema.Add(new ColumnInfo("Ch9DownloadEventsJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("IsCh9EmbeddedPageView", ColumnDataType.BooleanQ));
            schema.Add(new ColumnInfo("IsSelectLocale", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("IsF1Query",ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("QuickfilterJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("TutorialJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("GitHubIssueJson", ColumnDataType.String));
            schema.Add(new ColumnInfo("IsGitHubIssue", ColumnDataType.Boolean));
            schema.Add(new ColumnInfo("VideoInformation", ColumnDataType.String));
            schema.Add(new ColumnInfo("VideoDuration", ColumnDataType.String));
            schema.Add(new ColumnInfo("MSUserAlias", ColumnDataType.String) { Source = leftSchema[leftSchema["MSUserAlias"]] });
            schema.Add(new ColumnInfo("AppId", ColumnDataType.String) { Source = leftSchema[leftSchema["AppId"]] });

            return schema;
        }

        public override IEnumerable<Row> Combine(RowSet left, RowSet right, Row outputRow, string[] args)
        {
            RowList rawEvents = new RowList();
            rawEvents.Load(right);
            Row previous_head = null;

            foreach (Row head in left.Rows)
            {
                outputRow.Reset();
                if (head != null)
                {
                    ExtractDataFromHeadEvent(outputRow, head);

                    var correlatedRawEvents = rawEvents.Rows.Where(e => e["server_utc_datetime"].DateTime >= head["server_utc_datetime"].DateTime).Take(MaxEventCount).OrderBy(ele => ele["server_utc_datetime"].DateTime).ToList();
                    if (previous_head != null)
                        correlatedRawEvents = rawEvents.Rows.Where(e => e["server_utc_datetime"].DateTime >= head["server_utc_datetime"].DateTime &&
                            e["server_utc_datetime"].DateTime < previous_head["server_utc_datetime"].DateTime).Take(MaxEventCount).OrderBy(ele => ele["server_utc_datetime"].DateTime).ToList();
                    if (correlatedRawEvents != null)
                    {
                        outputRow["EventCount"].Set(correlatedRawEvents.Count() + 1);

                        var actions = new List<Action>();
                        actions.Add(() => { previous_head = head.Clone(); });
                        actions.Add(() => { CalcProp_TimingEvent(outputRow, head, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_PageViewLength(outputRow, head, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_ContentLang(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_CopyEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_ClickEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_APISearch(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_ScrollEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_SwitcherEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_TargetUrl(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_ThemeChangeEvent(outputRow, head, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_CloudShellEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_CliLoginEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_SiteSearchEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_Ch9(outputRow,head, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_TabEvent(outputRow, head, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_LocaleSearchEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_QuickFilterEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_TutorialEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_GitHubIssueEvent(outputRow, correlatedRawEvents); });
                        actions.Add(() => { CalcProp_MediaStreamVideoEvent(outputRow, head, correlatedRawEvents); });
                        Parallel.Invoke(new ParallelOptions() { MaxDegreeOfParallelism = 4 }, actions.ToArray());

                        // IsClick
                        outputRow["IsClickedComments"].Set(correlatedRawEvents.Any(ele => JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray).Equals("comments", StringComparison.InvariantCultureIgnoreCase)));
                        outputRow["IsClickedDownloadPDF"].Set(correlatedRawEvents.Any(ele => JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray).Equals("downloadPdf", StringComparison.InvariantCultureIgnoreCase)));
                        outputRow["IsClickedEdit"].Set(correlatedRawEvents.Any(ele => JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray).Equals("edit", StringComparison.InvariantCultureIgnoreCase)));
                        outputRow["IsClickedFreeAccount"].Set(
                            correlatedRawEvents.Any(ele => !string.IsNullOrEmpty(ele["target_url"].String) &&
                            Regex.Match(ele["target_url"].String, "azure.microsoft.com/[a-z]{2}-[a-z]{2}/free", RegexOptions.IgnoreCase).Success)
                        );
                        outputRow["IsClickedMoniker"].Set(
                            correlatedRawEvents.Any(ele => JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "name", JsllPageViewHelper.JsonType.JsonArray).Equals("moniker", StringComparison.InvariantCultureIgnoreCase)) ||
                            correlatedRawEvents.Any(ele => JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "name", JsllPageViewHelper.JsonType.JsonArray).Equals("product", StringComparison.InvariantCultureIgnoreCase)));
                        outputRow["IsClickedShare"].Set(correlatedRawEvents.Any(ele => JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray).Equals("share", StringComparison.InvariantCultureIgnoreCase)));
                        outputRow["IsClickedTheme"].Set(correlatedRawEvents.Any(ele => JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "name", JsllPageViewHelper.JsonType.JsonArray).Equals("select-theme", StringComparison.InvariantCultureIgnoreCase)));
                        outputRow["IsClickedTOC"].Set(correlatedRawEvents.Any(ele => JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "lineage", JsllPageViewHelper.JsonType.JsonArray).Contains("left toc")));
                        if (!string.IsNullOrEmpty(outputRow["Locale"].String) && !string.IsNullOrEmpty(outputRow["Contentlang"].String))
                            outputRow["IsLocFallback"].Set(!outputRow["Locale"].String.Contains(outputRow["Contentlang"].String));

                        outputRow["IsBounce"].Set(outputRow["PageViewLength"].Integer < 5 &&
                            outputRow["CopyEvents"].Integer == 0 && outputRow["LinkClickEvents"].Integer == 0 && outputRow["SwitcherEvents"].Integer == 0 && outputRow["target_url_events"].Integer == 0 && outputRow["MaxScroll"].Integer == 0);

                        outputRow["ProcessDateTime"].Set(DateTime.UtcNow);
                        outputRow["IsSelectLocale"].Set(correlatedRawEvents.Any(ele => JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray).Equals("select-locale",StringComparison.InvariantCultureIgnoreCase)));
                    }
                }
                yield return outputRow;
            }
        }
        private static void CalcProp_MediaStreamVideoEvent(Row outputRow,Row head, List<Row> correlatedRawEvents)
        {
            if (head["enrich_url_pg_domain"]!=null && !string.IsNullOrEmpty(head["enrich_url_pg_domain"].String) && head["enrich_url_pg_domain"].String.Equals("mediastream.microsoft.com",StringComparison.InvariantCultureIgnoreCase))
            {
                var videoInfo = correlatedRawEvents.Where(ele => ele["Content"] != null && !string.IsNullOrEmpty(ele["Content"].String) && !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "vidnm", JsllPageViewHelper.JsonType.JsonArray)));
                if (videoInfo.Any())
                {
                    List<MediastreamVideoInformation> videoInfoModel = new List<MediastreamVideoInformation>();
                    foreach (var e in videoInfo)
                    {
                        videoInfoModel.Add(new MediastreamVideoInformation()
                        {
                            Cid = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "cid", JsllPageViewHelper.JsonType.JsonArray),
                            Ru = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "ru", JsllPageViewHelper.JsonType.JsonArray),
                            StreamId = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "StreamId", JsllPageViewHelper.JsonType.JsonArray),
                            IsLive = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "isLive", JsllPageViewHelper.JsonType.JsonArray),
                            MpsChannel = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "mpsChannel", JsllPageViewHelper.JsonType.JsonArray),
                            MpsEventId = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "mpsEventId", JsllPageViewHelper.JsonType.JsonArray),
                            MpsSourceId = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "mpsSourceId", JsllPageViewHelper.JsonType.JsonArray),
                            Cdn = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "cdn", JsllPageViewHelper.JsonType.JsonArray),
                            ProtectionType = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "ProtectionType", JsllPageViewHelper.JsonType.JsonArray),
                            Ps = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "ps", JsllPageViewHelper.JsonType.JsonArray),
                            Vidnm = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "vidnm", JsllPageViewHelper.JsonType.JsonArray),
                            Viddur = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "viddur", JsllPageViewHelper.JsonType.JsonArray),
                            Campaignid = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "campaignid", JsllPageViewHelper.JsonType.JsonArray),
                            FirstTrackDate = e["server_utc_datetime"].DateTime
                        });
                    }
                    var videoInfoModelSort = videoInfoModel.OrderBy(e => e.FirstTrackDate).ToList();
                    var firstTractVideoInfo = from video in videoInfoModelSort
                                              group video by video.Vidnm into gp
                                              select gp.First();

                    outputRow["VideoInformation"].Set(JsonConvert.SerializeObject(firstTractVideoInfo));
                }
                var pbsEvents = correlatedRawEvents.Where(ele => ele["Content"] != null && !string.IsNullOrEmpty(ele["Content"].String) && JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "event", JsllPageViewHelper.JsonType.JsonArray).Equals("pbs", StringComparison.InvariantCultureIgnoreCase));
                if (pbsEvents.Any())
                {
                    List<MediastreamVideoDuration> videoDuration = new List<MediastreamVideoDuration>();
                    foreach (var e in pbsEvents)
                    {
                        videoDuration.Add(new MediastreamVideoDuration()
                        {
                            Vidnm = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "vidnm", JsllPageViewHelper.JsonType.JsonArray),
                            VidDuration = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "currentPlayTime", JsllPageViewHelper.JsonType.JsonArray)
                        });
                    }
                    float temp = 0;
                    var viddur = from video in videoDuration
                                 group video by video.Vidnm into gr
                                 select new
                                 {
                                     Vidnm = gr.Key,
                                     VidDuration = gr.Sum(e => float.TryParse(e.VidDuration, out temp) ? temp : 0).ToString()
                                 };

                    outputRow["VideoDuration"].Set(JsonConvert.SerializeObject(viddur));
                }
            }
        }
        private static void CalcProp_GitHubIssueEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var events = correlatedRawEvents.Where(ele => ele["Content"]!=null && !string.IsNullOrEmpty(ele["Content"].String)&&(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "type", JsllPageViewHelper.JsonType.JsonArray).Equals("github-issue-created",StringComparison.InvariantCultureIgnoreCase) || JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "type", JsllPageViewHelper.JsonType.JsonArray).Equals("github-comment-created", StringComparison.InvariantCultureIgnoreCase)));
            long temp = -1;
            List<GitHubIssueModel> models = new List<GitHubIssueModel>();
            foreach (var e in events)
            {
                if (JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "type", JsllPageViewHelper.JsonType.JsonArray).Equals("github-issue-created", StringComparison.InvariantCultureIgnoreCase))
                {
                    string type = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "type", JsllPageViewHelper.JsonType.JsonArray);
                    string repo = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "repo", JsllPageViewHelper.JsonType.JsonArray);
                    long id = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "id", JsllPageViewHelper.JsonType.JsonArray), out temp) ? temp : -1;
                    long number = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "number", JsllPageViewHelper.JsonType.JsonArray), out temp) ? temp : -1;
                    string title = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "title", JsllPageViewHelper.JsonType.JsonArray);
                    string user = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "user", JsllPageViewHelper.JsonType.JsonArray);
                    string author_Association = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "author_association", JsllPageViewHelper.JsonType.JsonArray);
                    models.Add(new GitHubIssueModel()
                    {
                        Type = type,
                        Repo = repo,
                        Id = id,
                        Number = number,
                        Title = title,
                        User = user,
                        Author_Association = author_Association,
                        Url = string.Format("https://github.com/{0}/issues/{1}#issue-{2}", repo, number.ToString(), id.ToString()).ToLower(),
                        EventDateTime = e["server_utc_datetime"].DateTime
                    });
                }
                else
                {
                    string type = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "type", JsllPageViewHelper.JsonType.JsonArray);
                    string repo = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "repo", JsllPageViewHelper.JsonType.JsonArray);
                    long id = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "id", JsllPageViewHelper.JsonType.JsonArray), out temp) ? temp : -1;
                    long number = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "issueNumber", JsllPageViewHelper.JsonType.JsonArray), out temp) ? temp : -1;
                    string title = string.Empty;
                    string user = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "user", JsllPageViewHelper.JsonType.JsonArray);
                    string author_Association = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "author_association", JsllPageViewHelper.JsonType.JsonArray);
                    models.Add(new GitHubIssueModel()
                    {
                        Type = type,
                        Repo = repo,
                        Id = id,
                        Number = number,
                        Title = title,
                        User = user,
			User = user,
                        Author_Association = author_Association,
                        Url = string.Format("https://github.com/{0}/issues/{1}#issuecomment-{2}", repo, number.ToString(), id.ToString()).ToLower(),
                        EventDateTime = e["server_utc_datetime"].DateTime
                    });
                }
            }
            if (models.Any())
                outputRow["GitHubIssueJson"].Set(JsonConvert.SerializeObject(models));
            outputRow["IsGitHubIssue"].Set(events.Any());

        }

        private static void CalcProp_QuickFilterEvent(Row outputRow,List<Row> correlatedRawEvents)
        {
            var events = correlatedRawEvents.Where(ele => (ele["CustomEvent"] != null && ele["CustomEvent"].String.Equals("api-browser-quickfilter", StringComparison.InvariantCultureIgnoreCase)));

            List<QuickfilterModel> models = new List<QuickfilterModel>();
            foreach (var e in events)
            {
                models.Add(new QuickfilterModel()
                {                  
                    Quickfilter = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "value", JsllPageViewHelper.JsonType.JsonArray),
                    QuickfilterPlatform = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "platform", JsllPageViewHelper.JsonType.JsonArray),
                    EventDateTime = e["server_utc_datetime"].DateTime
                });
            }
            if (models.Any())
                outputRow["QuickfilterJson"].Set(JsonConvert.SerializeObject(models));
        }

        private static void CalcProp_TutorialEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var events = correlatedRawEvents.Where(ele => (ele["Content"]!=null && JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray).StartsWith("tutorial-run"))).OrderBy(e=>e["server_utc_datetime"].DateTime);
            List<TutorialModel> models = new List<TutorialModel>();
            foreach (var e in events)
            {
                    models.Add(new TutorialModel()
                    {
                        TutorialRun = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray),
                        Url = string.Format("https://{0}{1}", e["enrich_url_pg_domain"]!=null && !string.IsNullOrEmpty(e["enrich_url_pg_domain"].String)?e["enrich_url_pg_domain"].String:string.Empty, e["enrich_url_pg_uri_stem"]!=null && !string.IsNullOrEmpty(e["enrich_url_pg_uri_stem"].String)?e["enrich_url_pg_uri_stem"].String:string.Empty).ToLower(),
                        EventDateTime = e["server_utc_datetime"].DateTime
                    });
            }
            if (models.Any())
                outputRow["TutorialJson"].Set(JsonConvert.SerializeObject(models));
        }
        private static void CalcProp_TimingEvent(Row outputRow, Row head, List<Row> correlatedRawEvents)
        {
            
                var contentUpdatedEvent = correlatedRawEvents.Where(ele =>
                    ele["event_type"].String == "7" &&
                    !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["custom_tag_list"].String, "timing", JsllPageViewHelper.JsonType.JsonObject)));
                if (contentUpdatedEvent.Any())
                {
                    try { 
                            var updatedevent = contentUpdatedEvent.FirstOrDefault();
                            string perfTimingData = JsllPageViewHelper.GetValueFromJsonString(updatedevent, updatedevent["custom_tag_list"].String, "timing", JsllPageViewHelper.JsonType.JsonObject);
                            List<long> timingList = new List<long>();
                            long timvalue = 0L;
                            long loadEventEnd = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "loadEventEnd", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(loadEventEnd);
                            long loadEventStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "loadEventStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(loadEventStart);
                            long domComplete = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "domComplete", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(domComplete);
                            long domContentLoadedEventEnd = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "domContentLoadedEventEnd", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(domContentLoadedEventEnd);
                            long domContentLoadedEventStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "domContentLoadedEventStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(domContentLoadedEventStart);
                            long domInteractive = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "domInteractive", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(domInteractive);
                            long domLoading = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "domLoading", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(domLoading);
                            long responseEnd = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "responseEnd", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(responseEnd);
                            long responseStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "responseStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(responseStart);
                            long requestStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "requestStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(requestStart);
                            long secureConnectionStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "secureConnectionStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(secureConnectionStart);
                            long connectEnd = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "connectEnd", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(connectEnd);
                            long connectStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "connectStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(connectStart);
                            long domainLookupEnd = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "domainLookupEnd", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(domainLookupEnd);
                            long domainLookupStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "domainLookupStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(domainLookupStart);
                            long fetchStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "fetchStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(fetchStart);
                            long redirectEnd = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "redirectEnd", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(redirectEnd);
                            long redirectStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "redirectStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(redirectStart);
                            long unloadEventEnd = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "unloadEventEnd", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(unloadEventEnd);
                            long unloadEventStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "unloadEventStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(unloadEventStart);
                            long navigationStart = long.TryParse(JsllPageViewHelper.GetValueFromJsonString(updatedevent, perfTimingData, "navigationStart", JsllPageViewHelper.JsonType.JsonObject), out timvalue) ? timvalue : -1;
                            timingList.Add(navigationStart);
                            var d = new PerfTimingModel()
                            {
                                EventDateTime = head["server_utc_datetime"].DateTime,
                                RedirectStart = redirectStart,
                                RedirectEnd = redirectEnd,
                                FetchStart = fetchStart,
                                ConnectStart = connectStart,
                                ConnectEnd = connectEnd,
                                RequestStart = requestStart,
                                ResponseStart = responseStart,
                                ResponseEnd = responseEnd,
                                DomLoading = domLoading,
                                DomInteractive = domInteractive,
                                DomContentLoadedEventStart = domContentLoadedEventStart,
                                DomContentLoadedEventEnd = domContentLoadedEventEnd,
                                DomComplete = domComplete,
                                LoadEventStart = loadEventStart,
                                LoadEventEnd = loadEventEnd,
                                UnloadEventStart = unloadEventStart,
                                UnloadEventEnd = unloadEventEnd,
                                DomainLookupStart = domainLookupStart,
                                DomainLookupEnd = domainLookupEnd,
                                SecureConnectionStart = secureConnectionStart,
                                NavigationStart = navigationStart,
                                PageLoadTime = timingList.Where(ele => ele > -1).Max()
                            };
                            outputRow["PerfTimingEventsJson"].Set(JsonConvert.SerializeObject(d));
                            outputRow["PageLoadTime"].Set(d.PageLoadTime);
                    }
                    catch { }
            }
            else if(!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, head["custom_tag_list"].String, "ms.perf.timing", JsllPageViewHelper.JsonType.JsonObject)))
            {
                try
                {
                    int value;
                    var perfTimingData = JsllPageViewHelper.GetValueFromJsonString(head, head["custom_tag_list"].String, "ms.perf.timing", JsllPageViewHelper.JsonType.JsonObject).Split(',').Select(ele => int.TryParse(ele, out value) ? value : -1).ToList();
                    var d = new PerfTimingModel()
                    {
                    EventDateTime = head["server_utc_datetime"].DateTime,
                    RedirectStart = perfTimingData[0],
                    RedirectEnd = perfTimingData[1],
                    FetchStart = perfTimingData[2],
                    ConnectStart = perfTimingData[3],
                    ConnectEnd = perfTimingData[4],
                    RequestStart = perfTimingData[5],
                    ResponseStart = perfTimingData[6],
                    ResponseEnd = perfTimingData[7],
                    DomLoading = perfTimingData[8],
                    DomInteractive = perfTimingData[9],
                    DomContentLoadedEventStart = perfTimingData[10],
                    DomContentLoadedEventEnd = perfTimingData[11],
                    DomComplete = perfTimingData[12],
                    LoadEventStart = perfTimingData[13],
                    LoadEventEnd = perfTimingData[14],
                    PageLoadTime = perfTimingData.Where(ele => ele > -1).Max()
                    };
                    outputRow["PerfTimingEventsJson"].Set(JsonConvert.SerializeObject(d));
                    outputRow["PageLoadTime"].Set(d.PageLoadTime);
            }
                catch { }
            }
            else
                outputRow["PageLoadTime"].Set(-1); // default to -1
        }
        private static void CalcProp_LocaleSearchEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var events = correlatedRawEvents.Where(ele => (ele["CustomEvent"] !=null && ele["CustomEvent"].String.Equals("localesearch", StringComparison.InvariantCultureIgnoreCase))||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "region", JsllPageViewHelper.JsonType.JsonArray)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "term", JsllPageViewHelper.JsonType.JsonArray)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "results", JsllPageViewHelper.JsonType.JsonArray)));

            List<LocaleSearchModel> models = new List<LocaleSearchModel>();
            foreach (var e in events)
            {
                models.Add(new LocaleSearchModel()
                {
                    EventDateTime = e["server_utc_datetime"].DateTime,
                    Region = JsllPageViewHelper.GetValueFromJsonString(e,e["Content"].String, "region", JsllPageViewHelper.JsonType.JsonArray),
                    Term = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "term", JsllPageViewHelper.JsonType.JsonArray),
                    Results = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "results", JsllPageViewHelper.JsonType.JsonArray)
                });
            }
            if (models.Any())
                outputRow["LocaleSearchEventsJson"].Set(JsonConvert.SerializeObject(models));

            outputRow["IsClickedLocaleSearch"].Set(events.Any());
        }
        private static void CalcProp_Ch9(Row outputRow, Row head,List<Row> correlatedRawEvents)
        {
            if (head["enrich_url_pg_domain"].String.Equals("channel9.msdn.com", StringComparison.InvariantCultureIgnoreCase))
            {
                string asst = string.Empty;
                DateTime playButton = DateTime.MinValue;
                DateTime videoStart = DateTime.MinValue;
                asst = JsllPageViewHelper.GetValueFromJsonString(head, head["custom_tag_list"].String, "asst", JsllPageViewHelper.JsonType.JsonObject);
                if (string.IsNullOrEmpty(asst))
                {
                    var asstevents = correlatedRawEvents.Where(ele => !string.IsNullOrEmpty(ele["custom_tag_list"].String) && !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["custom_tag_list"].String, "asst", JsllPageViewHelper.JsonType.JsonObject))).OrderBy(ele => ele["server_utc_datetime"].DateTime);
                    if (asstevents.Any())
                        asst = JsllPageViewHelper.GetValueFromJsonString(asstevents.FirstOrDefault(), asstevents.FirstOrDefault()["custom_tag_list"].String, "asst", JsllPageViewHelper.JsonType.JsonObject);
                }
                outputRow["Ch9Asst"].Set(asst);
                outputRow["Ch9SessionTags"].Set(string.Format("pageDomain={0}&pageUrl={1}{2}?c3.player.name=html5+website+player", head["enrich_url_pg_domain"].String, head["enrich_url_pg_domain"].String, head["enrich_url_pg_uri_stem"].String));

                var playButtonClickEvents = correlatedRawEvents.Where(ele => ele["Content"] != null && !string.IsNullOrEmpty(ele["Content"].String) && JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray).Trim().StartsWith("Play") 
                //&& !string.IsNullOrEmpty(head["pg_title"].String) && head["pg_title"].String.Contains('|') &&
                //JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray).Trim().Replace(" ","").ToLower().Contains(head["pg_title"].String.Substring(0, head["pg_title"].String.IndexOf('|')).Trim().Replace(" ","").ToLower())
                ).OrderBy(ele => ele["server_utc_datetime"].DateTime);
                if (playButtonClickEvents.Any())
                {
                    playButton = playButtonClickEvents.FirstOrDefault()["server_utc_datetime"].DateTime;
                }
                var videoStartEvents = correlatedRawEvents.Where(ele => ele["Behavior"].IntegerQ != null && ele["Behavior"].IntegerQ.Equals(240)).OrderBy(ele => ele["server_utc_datetime"].DateTime);
                if (videoStartEvents.Any())
                {
                    videoStart = videoStartEvents.FirstOrDefault()["server_utc_datetime"].DateTime;
                }
                if (playButton != DateTime.MinValue && videoStart != DateTime.MinValue)
                {
                    outputRow["Ch9VideoPlayDelay"].Set((videoStart - playButton).TotalMilliseconds);
                }
                outputRow["Ch9VideoPlayDateTime"].Set(playButton == DateTime.MinValue ? (DateTime?)null : playButton);

                outputRow["IsCh9EmbeddedPageView"].Set(head["enrich_url_pg_uri_stem"].String.Trim().EndsWith("/player"));
                var downloadEvents = correlatedRawEvents.Where(ele => ele["Behavior"].IntegerQ != null && ele["Behavior"].IntegerQ.Equals(41)).OrderBy(ele => ele["server_utc_datetime"].DateTime);
                if (downloadEvents.Any())
                {
                    List<Ch9DownloadEventsModel> Ch9DownloadEventsModels = new List<Ch9DownloadEventsModel>();
                    foreach (var e in downloadEvents)
                    {
                        Ch9DownloadEventsModels.Add(new Ch9DownloadEventsModel()
                        {
                            EventDateTime = e["server_utc_datetime"].DateTime,
                            VideoName = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "dlnm", JsllPageViewHelper.JsonType.JsonArray),
                            DownloadType = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "dltype", JsllPageViewHelper.JsonType.JsonArray)
                        });
                    }
                    outputRow["Ch9DownloadEventsJson"].Set(JsonConvert.SerializeObject(Ch9DownloadEventsModels));
                }

                var videoEvents = correlatedRawEvents.Where(ele => !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "viddur", JsllPageViewHelper.JsonType.JsonArray))
                || !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "vidpct", JsllPageViewHelper.JsonType.JsonArray))
                || !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "vidwt", JsllPageViewHelper.JsonType.JsonArray)));
                if (videoEvents.Any())
                {
                    List<Channel9VideoModel> Channel9VideoModels = new List<Channel9VideoModel>();

                    foreach (var e in videoEvents)
                    {
                        string vidpct = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "vidpct", JsllPageViewHelper.JsonType.JsonArray);
                        string viddur = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "viddur", JsllPageViewHelper.JsonType.JsonArray);
                        string vidwt = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "vidwt", JsllPageViewHelper.JsonType.JsonArray);

                        double completionrateValue = 0;
                        completionrateValue = string.IsNullOrEmpty(vidpct) ? 0 : Convert.ToDouble(vidpct);
                        TimeSpan lengthValue = TimeSpan.Zero;
                        TimeSpan watchTimeValue = TimeSpan.Zero;
                        try
                        {
                            lengthValue = TimeSpan.FromSeconds(string.IsNullOrEmpty(viddur) ? 0 : Convert.ToDouble(viddur));
                            watchTimeValue = TimeSpan.FromSeconds(string.IsNullOrEmpty(vidwt) ? 0 : Convert.ToDouble(vidwt));
                        }
                        catch (Exception ex)
                        {
                            lengthValue = TimeSpan.Zero;
                            watchTimeValue = TimeSpan.Zero;
                            ScopeRuntime.Diagnostics.DebugStream.WriteLine("Epoch=" + e["Epoch"].String + ";Sequence=" + e["Sequence"].String + ";Error=" + ex.Message);
                        }
                        Channel9VideoModels.Add(new Channel9VideoModel()
                        {
                            EventDateTime = e["server_utc_datetime"].DateTime,
                            completionrate = completionrateValue,
                            length = lengthValue,
                            watchtime = watchTimeValue
                        });
                    }

                    if (Channel9VideoModels.Any())
                    {
                        outputRow["Ch9VideoEventsJson"].Set(JsonConvert.SerializeObject(Channel9VideoModels));
                        outputRow["Ch9VideoCompletionRate"].Set(Channel9VideoModels.Max(ele => ele.completionrate));
                        outputRow["Ch9VideoLength"].Set(Channel9VideoModels.Max(ele => ele.length).ToString("g"));
                        outputRow["Ch9VideoWatchtime"].Set(Channel9VideoModels.Max(ele => ele.watchtime).ToString("g"));
                    }
                }
            }
        }

        private static void CalcProp_TabEvent(Row outputRow, Row head, List<Row> correlatedRawEvents)
        {
            int pvLength = 0;
            if (correlatedRawEvents.Any())
                pvLength = (Int32)(correlatedRawEvents.Max(e => e["server_utc_datetime"].DateTime) - head["server_utc_datetime"].DateTime).TotalSeconds;

            var events = correlatedRawEvents.Where(ele =>
               ele["target_url"] != null && !string.IsNullOrEmpty(ele["target_url"].String));

            Uri eventTargetUri = null;
            string eventUriStem = "";
            string eventUriQuery = "";
            DateTime previousDate = DateTime.MinValue;
            string previousTab = string.Empty;
            string defTab = string.Empty;
            string tab = string.Empty;
            Dictionary<string, int> tabDic = new Dictionary<string, int>();
            DefaultTabbed defaultTabJson = new DefaultTabbed();
            List<Tabs> tabsJson = new List<Tabs>();
            TabbedContentModel tabModel = new TabbedContentModel();

            if (head["enrich_url_pg_query_string"] != null && !string.IsNullOrEmpty(head["enrich_url_pg_query_string"].String) && head["enrich_url_pg_query_string"].String.Contains("tabs="))
            {
                defTab = JsllPageViewHelper.GetTabName(head["enrich_url_pg_query_string"].String, "tabs=");
                previousTab = defTab;
                previousDate = head["server_utc_datetime"].DateTime;
                tabDic.Add(defTab, 0);
                if (events.Any())
                {
                    foreach (var e in correlatedRawEvents)
                    {                     
                        tabDic[previousTab] = tabDic[previousTab] + (Int32)(e["server_utc_datetime"].DateTime - previousDate).TotalSeconds;                     

                        if (e["target_url"] != null && !string.IsNullOrEmpty(e["target_url"].String) && JsllPageViewHelper.IsValidUri(e["target_url"].String, ref eventTargetUri))
                        {
                            eventUriStem = string.Join("", eventTargetUri.Segments);
                            eventUriQuery = eventTargetUri.Query;

                            string targetTabName = JsllPageViewHelper.GetTabName(e["target_url"].String, "tabs=");
                            string targetTabpanelName = JsllPageViewHelper.GetTabName(e["target_url"].String, "tabpanel_");
                            tab = !string.IsNullOrEmpty(targetTabpanelName) ? targetTabpanelName : targetTabName;

                            if (!string.IsNullOrEmpty(tab))
                            {
                                bool isContains = false;
                                foreach (string key in tabDic.Keys)
                                {
                                    if (key.Contains(tab) && tab.Length != key.Length)
                                    {
                                        tab = key;
                                        isContains = true;
                                        break;
                                    }
                                }
                                if (!isContains && !tabDic.ContainsKey(tab))
                                {
                                    tabDic.Add(tab, 0);
                                }
                                previousTab = tab;
                            }
                        }
                        previousDate = e["server_utc_datetime"].DateTime;
                    }
                    foreach (KeyValuePair<string, int> t in tabDic)
                    {
                        if (t.Key.Equals(defTab, StringComparison.InvariantCultureIgnoreCase))
                        {
                            defaultTabJson.default_tab_name = t.Key;
                            defaultTabJson.deault_tab_duration = t.Value;
                        }
                        else
                        {
                            tabsJson.Add(new Tabs()
                            {
                                tab_name = t.Key,
                                tab_duration = t.Value
                            });
                        }

                    }
                }
                else
                {
                    defaultTabJson.default_tab_name = defTab;
                    defaultTabJson.deault_tab_duration = pvLength;
                }
                tabModel.DefaultTab = defaultTabJson;
                tabModel.Tab = tabsJson;
                outputRow["TabbedEventsJson"].Set(JsonConvert.SerializeObject(tabModel));
            }
            outputRow["IsTabbedEvents"].Set(!string.IsNullOrEmpty(defTab));
        }

        private static void CalcProp_SiteSearchEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var events = correlatedRawEvents.Where(ele => ele["CustomEvent"]!=null &&
                ele["CustomEvent"].String.Equals("uhf-search-results",StringComparison.InvariantCultureIgnoreCase));

            List<SiteSearchModel> models = new List<SiteSearchModel>();
            foreach (var e in events)
            {
                models.Add(new SiteSearchModel()
                {
                    EventDateTime = e["server_utc_datetime"].DateTime,
                    term = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "term",JsllPageViewHelper.JsonType.JsonArray),
                    resultCount = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "results", JsllPageViewHelper.JsonType.JsonArray),
                    skipCount = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "skip", JsllPageViewHelper.JsonType.JsonArray)
                });
            }
            if (models.Any())
                outputRow["SiteSearchEventsJson"].Set(JsonConvert.SerializeObject(models));

            outputRow["IsClickedSiteSearch"].Set(events.Any());
        }

        private static void CalcProp_CliLoginEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var loginEvents = correlatedRawEvents.Where(ele => ele["CustomEvent"]!=null&& ele["CustomEvent"].String.Equals("azure-cli-login",StringComparison.InvariantCultureIgnoreCase));
            List<AzureCliLoginModel> azureCliLoginModels = new List<AzureCliLoginModel>();
            foreach (var e in loginEvents)
            {
                azureCliLoginModels.Add(new AzureCliLoginModel()
                {
                    EventDateTime = e["server_utc_datetime"].DateTime,
                    Content = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "scnstp", JsllPageViewHelper.JsonType.JsonArray),
                });
            }
            if (azureCliLoginModels.Any())
                outputRow["AzureCliLogninEventsJson"].Set(JsonConvert.SerializeObject(azureCliLoginModels));
        }
        //PENDING
        private static void CalcProp_CloudShellEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var cloudShellEvents = correlatedRawEvents.Where(ele => JsllPageViewHelper.GetValueFromJsonString(ele,ele["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray).StartsWith("code-header-try-it"));
            outputRow["IsClickedCloudShell"].Set(cloudShellEvents.Any());

            List<CloudShellModel> cloudShellModels = new List<CloudShellModel>();
            foreach (var e in cloudShellEvents)
            {
                cloudShellModels.Add(new CloudShellModel()
                {
                    EventDateTime = e["server_utc_datetime"].DateTime,
                    Content = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "contentName", JsllPageViewHelper.JsonType.JsonArray),
                });
            }
            if (cloudShellModels.Any())
                outputRow["CloudShellEventsJson"].Set(JsonConvert.SerializeObject(cloudShellModels));
        }

        private static void CalcProp_ThemeChangeEvent(Row outputRow, Row head, List<Row> correlatedRawEvents)
        {
            var theme_firstString = "";
            if (!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, head["custom_tag_list"].String, "theme", JsllPageViewHelper.JsonType.JsonObject)))
                theme_firstString = JsllPageViewHelper.GetValueFromJsonString(head, head["custom_tag_list"].String, "theme", JsllPageViewHelper.JsonType.JsonObject);
            else
                theme_firstString = JsllPageViewHelper.GetValueFromJsonString(head, head["custom_tag_list"].String, "ms.theme", JsllPageViewHelper.JsonType.JsonObject);
            if (!string.IsNullOrEmpty(theme_firstString))
                outputRow["Theme_First"].Set(theme_firstString);
            else
            {
                var theme_first = correlatedRawEvents.FirstOrDefault(ele => !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele,ele["custom_tag_list"].String, "theme", JsllPageViewHelper.JsonType.JsonObject))|| !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["custom_tag_list"].String, "ms.theme", JsllPageViewHelper.JsonType.JsonObject)));
                if (theme_first != null)
                    outputRow["Theme_First"].Set(!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(theme_first,theme_first["custom_tag_list"].String, "theme", JsllPageViewHelper.JsonType.JsonObject))? JsllPageViewHelper.GetValueFromJsonString(theme_first, theme_first["custom_tag_list"].String, "theme", JsllPageViewHelper.JsonType.JsonObject):JsllPageViewHelper.GetValueFromJsonString(theme_first, theme_first["custom_tag_list"].String, "ms.theme", JsllPageViewHelper.JsonType.JsonObject));
            }

            var theme_last = correlatedRawEvents.LastOrDefault(ele => !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele,ele["custom_tag_list"].String, "theme", JsllPageViewHelper.JsonType.JsonObject)) || !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["custom_tag_list"].String, "ms.theme", JsllPageViewHelper.JsonType.JsonObject)));
            if (theme_last != null)
                outputRow["Theme_Last"].Set(!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(theme_last,theme_last["custom_tag_list"].String, "theme", JsllPageViewHelper.JsonType.JsonObject))? JsllPageViewHelper.GetValueFromJsonString(theme_last, theme_last["custom_tag_list"].String, "theme", JsllPageViewHelper.JsonType.JsonObject): JsllPageViewHelper.GetValueFromJsonString(theme_last, theme_last["custom_tag_list"].String, "ms.theme", JsllPageViewHelper.JsonType.JsonObject));
            else if (!string.IsNullOrEmpty(theme_firstString))
                outputRow["Theme_Last"].Set(theme_firstString);
        }

        private static void CalcProp_TargetUrl(Row outputRow, List<Row> correlatedRawEvents)
        {
            var target_urls = correlatedRawEvents.Select(ele => ele["target_url"].String).Where(ele => !string.IsNullOrEmpty(ele));

            outputRow["target_url_events"].Set(target_urls.Count());
            if (target_urls.Any())
                outputRow["target_url_Json"].Set(JsonConvert.SerializeObject(target_urls.ToList()));
        }

        private static void CalcProp_SwitcherEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var switcherEvents = correlatedRawEvents.Where(ele => ele["CustomEvent"].String.Equals("select-value-changed",StringComparison.InvariantCultureIgnoreCase));
            List<SwitcherEventModel> switcherModels = new List<SwitcherEventModel>();
            foreach (var e in switcherEvents)
            {
                switcherModels.Add(new SwitcherEventModel()
                {
                    EventDateTime = e["server_utc_datetime"].DateTime,
                    Switcher = JsllPageViewHelper.GetValueFromJsonString(e,e["Content"].String, "name",JsllPageViewHelper.JsonType.JsonArray),
                    SwitcherValue = JsllPageViewHelper.GetValueFromJsonString(e,e["Content"].String, "value", JsllPageViewHelper.JsonType.JsonArray)
                });
            }
            outputRow["SwitcherEvents"].Set(switcherModels.Count);
            if (switcherModels.Any())
                outputRow["SwitcherEventsJson"].Set(JsonConvert.SerializeObject(switcherModels));
        }

        private static void CalcProp_ScrollEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var scrollEvents = correlatedRawEvents.Where(ele => ele["ActionType"] !=null && !string.IsNullOrEmpty(ele["ActionType"].String) && ele["ActionType"].String.StartsWith("S"));
            List<ScrollEventModel> scrollEventModels = new List<ScrollEventModel>();
            var prev_scnum_int = -1;

            foreach (var e in scrollEvents)
            {
                var _scnum_int = 0;
                var _scnum = "";
                if (e["PageHeight"] != null && e["PageHeight"].LongQ!=null && e["VScroll"] != null && e["VScroll"].LongQ!=null && e["PageHeight"]!=null && e["PageHeight"].LongQ !=null && e["PageHeight"].LongQ != 0)
                {
                    _scnum_int = Convert.ToInt32(100.0*e["VScroll"].LongQ / e["PageHeight"].LongQ);
                }
                _scnum = string.Format("scroll-{0}%", _scnum_int.ToString());

                // skip repeating scnum events
                if (prev_scnum_int == -1 || prev_scnum_int != _scnum_int)
                {
                    scrollEventModels.Add(new ScrollEventModel()
                    {
                        EventDateTime = e["server_utc_datetime"].DateTime,
                        scnum = _scnum,
                        scnum_int = _scnum_int
                    });

                    prev_scnum_int = _scnum_int;
                }
            }
            outputRow["ScrollEvents"].Set(scrollEventModels.Count);
            if (scrollEventModels.Any())
            {
                outputRow["ScrollEventsJson"].Set(JsonConvert.SerializeObject(scrollEventModels));
                outputRow["MaxScroll"].Set(scrollEventModels.Max(ele => ele.scnum_int));
            }
            else
                outputRow["MaxScroll"].Set(0);
        }

        private static void CalcProp_PageViewLength(Row outputRow, Row head, List<Row> correlatedRawEvents)
        {
            //for docs 
            int totalBluredTime = 0;
            if (correlatedRawEvents.Any())
            {
                DateTime last_focusorblue_EventDateTime = DateTime.MinValue;
                foreach (var e in correlatedRawEvents.Where(ele => ele["CustomEvent"].String.Equals("page-focus-changed",StringComparison.InvariantCultureIgnoreCase)))
                {
                    if (JsllPageViewHelper.GetValueFromJsonString(e,e["Content"].String, "value", JsllPageViewHelper.JsonType.JsonArray).Equals("blur", StringComparison.InvariantCultureIgnoreCase) &&
                        last_focusorblue_EventDateTime == DateTime.MinValue)
                    {
                        last_focusorblue_EventDateTime = e["server_utc_datetime"].DateTime;
                    }
                    else if (JsllPageViewHelper.GetValueFromJsonString(e,e["Content"].String, "value", JsllPageViewHelper.JsonType.JsonArray).Equals("focus", StringComparison.InvariantCultureIgnoreCase) &&
                        last_focusorblue_EventDateTime != DateTime.MinValue)
                    {
                        totalBluredTime += Convert.ToInt32((e["server_utc_datetime"].DateTime - last_focusorblue_EventDateTime).TotalSeconds);
                        last_focusorblue_EventDateTime = DateTime.MinValue;
                    }
                }
                // include the last blur event
                if (last_focusorblue_EventDateTime != DateTime.MinValue)
                    totalBluredTime += Convert.ToInt32((correlatedRawEvents.Last()["server_utc_datetime"].DateTime - last_focusorblue_EventDateTime).TotalSeconds);
            }
            double pvLength = 0;
            if (correlatedRawEvents.Any())
                pvLength = (correlatedRawEvents.Max(e => e["server_utc_datetime"].DateTime) - head["server_utc_datetime"].DateTime).TotalSeconds;

            outputRow["PageViewLength"].Set(pvLength);
            outputRow["ActivePageViewLength"].Set(pvLength - totalBluredTime);
        }

        private static void CalcProp_ClickEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var linkClicksEvents = correlatedRawEvents.Where(ele =>
                ele["event_type"].String == "1" && (!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "ms.cmpgrp", JsllPageViewHelper.JsonType.JsonArray)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "ms.cmpnm", JsllPageViewHelper.JsonType.JsonArray)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(ele, ele["Content"].String, "ms.pgarea", JsllPageViewHelper.JsonType.JsonArray)) ||
                !string.IsNullOrEmpty(ele["target_url"].String)));
            List<LinkClickEventModel> linkClickModels = new List<LinkClickEventModel>();
            foreach (var e in linkClicksEvents)
            {
                string pageArea = string.Empty;
                string componentGroup = string.Empty;
                string componentName = string.Empty;
                string lineage = string.Empty;
                lineage = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "lineage", JsllPageViewHelper.JsonType.JsonArray);
                if (!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "ms.cmpgrp", JsllPageViewHelper.JsonType.JsonArray)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "ms.cmpnm", JsllPageViewHelper.JsonType.JsonArray)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "ms.pgarea", JsllPageViewHelper.JsonType.JsonArray)))
                {
                    componentGroup = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "ms.cmpgrp", JsllPageViewHelper.JsonType.JsonArray);
                    componentName = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "ms.cmpnm", JsllPageViewHelper.JsonType.JsonArray);
                    pageArea = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "ms.pgarea", JsllPageViewHelper.JsonType.JsonArray);
                }
                else if (!string.IsNullOrEmpty(lineage))
                {
                    string[] lineageAry = lineage.Split('>');
                    switch (lineageAry.Length)
                    {
                        case 1:
                            pageArea = lineageAry[0];
                            break;
                        case 2:
                            pageArea = lineageAry[1];
                            componentGroup = lineageAry[0];
                            break;
                        case 3:
                            pageArea = lineageAry[2];
                            componentName = lineageAry[0];
                            componentGroup = lineageAry[1];
                            break;
                        default:
                            pageArea = lineageAry[lineageAry.Length - 1];
                            componentName = lineageAry[0];
                            componentGroup = lineageAry[lineageAry.Length - 2];
                            break;
                    }
                }
                linkClickModels.Add(new LinkClickEventModel()
                {
                    EventDateTime = e["server_utc_datetime"].DateTime,
                    pgarea = pageArea,
                    cmpgrp = componentGroup,
                    cmpnm = componentName,
                    target_url = e["target_url"].String,
                    linkTitle = e["content_name"].String,
                    lineage= lineage
                });
            }
            outputRow["LinkClickEvents"].Set(linkClickModels.Count);
            if (linkClickModels.Any())
                outputRow["LinkClickEventsJson"].Set(JsonConvert.SerializeObject(linkClickModels));
        }

        private static void CalcProp_APISearch(Row outputRow, List<Row> correlatedRawEvents)
        {
            var apiSearchEvents = correlatedRawEvents.Where(ele => ele["CustomEvent"].String.Equals("api-browser-search",StringComparison.InvariantCultureIgnoreCase));
            outputRow["IsClickedAPISearch"].Set(apiSearchEvents.Any());
            List<APISearchModel> APISearchModels = new List<APISearchModel>();
            foreach (var e in apiSearchEvents)
            {
                int results = -1;
                APISearchModels.Add(new APISearchModel()
                {
                    EventDateTime = e["server_utc_datetime"].DateTime,
                    Moniker = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "moniker", JsllPageViewHelper.JsonType.JsonArray),
                    Results = int.TryParse(JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "results", JsllPageViewHelper.JsonType.JsonArray), out results) ? results : -1,
                    Term = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "term", JsllPageViewHelper.JsonType.JsonArray),
                });
            }
            if (APISearchModels.Any())
            {
                outputRow["APISearchEventsJson"].Set(JsonConvert.SerializeObject(APISearchModels));
                outputRow["APISearchLastMoniker"].Set(APISearchModels.Last().Moniker);
                outputRow["APISearchLastResults"].Set(APISearchModels.Last().Results);
                outputRow["APISearchLastTerm"].Set(APISearchModels.Last().Term);
            }
            outputRow["APISearchEvents"].Set(APISearchModels.Count);
        }

        private static void CalcProp_CopyEvent(Row outputRow, List<Row> correlatedRawEvents)
        {
            var copyEvents = correlatedRawEvents.Where(ele => ele["CustomEvent"].String.Equals("copy",StringComparison.InvariantCultureIgnoreCase));
            List<CopyEventModel> copyEventModels = new List<CopyEventModel>();
            foreach (var e in copyEvents)
            {
                var len = 0;
                copyEventModels.Add(new CopyEventModel()
                {
                    EventDateTime = e["server_utc_datetime"].DateTime,
                    copycontent = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "value", JsllPageViewHelper.JsonType.JsonArray),
                    copycontentlength = JsllPageViewHelper.GetValueFromJsonString(e, e["Content"].String, "value", JsllPageViewHelper.JsonType.JsonArray).Length
                });
            }
            outputRow["CopyEvents"].Set(copyEventModels.Count);
            if (copyEventModels.Any())
                outputRow["CopyEventsJson"].Set(JsonConvert.SerializeObject(copyEventModels));
            outputRow["IsClickedCopyButton"].Set(copyEventModels.Any());
        }

        private static void CalcProp_ContentLang(Row outputRow, List<Row> correlatedRawEvents)
        {
            var contentlangs = correlatedRawEvents.Select(ele => ele["Contentlang"].String).Where(ele => !string.IsNullOrEmpty(ele));
            if (contentlangs.Any())
            {
                var maxlen = contentlangs.Max(ele => ele.Length);
                var contentlang = contentlangs.FirstOrDefault(ele => ele.Length == maxlen);
                if (!string.IsNullOrEmpty(contentlang))
                    outputRow["Contentlang"].Set(contentlang.ToLower());
            }
        }

        private static void ExtractDataFromHeadEvent(Row outputRow, Row head)
        {
            // Identifiers
                head["Anid"].CopyTo(outputRow["Anid"]);
                head["ContentId"].CopyTo(outputRow["ContentId"]);
                head["MUID"].CopyTo(outputRow["MUID"]);
                head["PageViewId"].CopyTo(outputRow["PageViewId"]);
                head["puidhash"].CopyTo(outputRow["puidhash"]);
                head["TopicKey"].CopyTo(outputRow["TopicKey"]);
                head["mc1_visitor_id"].CopyTo(outputRow["VisitorId"]);
                head["enrich_rip_isp"].CopyTo(outputRow["enrich_rip_isp"]);
                head["enrich_url_pg_query_string"].CopyTo(outputRow["enrich_url_pg_query_string"]);
                head["enrich_session_id"].CopyTo(outputRow["enrich_session_id"]);
                head["CustomTagLocale"].CopyTo(outputRow["Locale"]);
                head["referrer_domain"].CopyTo(outputRow["ReferrerDomain"]);
                head["enrich_referrer_name"].CopyTo(outputRow["ReferrerName"]);
                head["enrich_referrer_type"].CopyTo(outputRow["ReferrerType"]);
                head["referrer_query_string"].CopyTo(outputRow["ReferrerQueryString"]);
                head["enrich_referrer_search_phrase"].CopyTo(outputRow["ReferrerSearchPhrase"]);
                head["referrer_uri_stem"].CopyTo(outputRow["Referrer_uri_stem"]);
                head["enrich_url_pg_domain"].CopyTo(outputRow["Site"]);
                head["server_utc_datetime"].CopyTo(outputRow["StartDateTime"]);
                head["pg_title"].CopyTo(outputRow["Title"]);
                head["custom_tag_list"].CopyTo(outputRow["custom_tag_list"]);
                head["enrich_ua_browser_name"].CopyTo(outputRow["UserBrowser"]);
                head["browser_lang"].CopyTo(outputRow["UserBrowserLang"]);
                head["client_ip"].CopyTo(outputRow["UserClientIP"]);
                head["enrich_ua_device"].CopyTo(outputRow["UserDevice"]);
                head["enrich_ua_os"].CopyTo(outputRow["UserOS"]);
                head["enrich_rip_continent"].CopyTo(outputRow["UserContinent"]);
                head["enrich_rip_country"].CopyTo(outputRow["UserCountry"]);
                head["enrich_rip_region"].CopyTo(outputRow["UserRegion"]);
                head["enrich_rip_state"].CopyTo(outputRow["UserState"]);
                head["enrich_rip_city"].CopyTo(outputRow["UserCity"]);
                head["enrich_rip_latitude"].CopyTo(outputRow["UserLatitude"]);
                head["enrich_rip_longitude"].CopyTo(outputRow["UserLongitude"]);
                head["ProductFamilyName"].CopyTo(outputRow["ProductFamilyName"]);
                head["ProductVersion"].CopyTo(outputRow["ProductVersion"]);
                head["event_id"].CopyTo(outputRow["head_event_id"]);
                head["IsUserConsent"].CopyTo(outputRow["IsUserConsent"]);
                head["ImpressionGuid"].CopyTo(outputRow["ImpressionGuid"]);
                head["enrich_rip_organization"].CopyTo(outputRow["enrich_rip_organization"]);
                head["enrich_rip_industry"].CopyTo(outputRow["enrich_rip_industry"]);
                head["enrich_is_bot"].CopyTo(outputRow["enrich_is_bot"]);
                head["MSUserAlias"].CopyTo(outputRow["MSUserAlias"]);
                head["AppId"].CopyTo(outputRow["AppId"]);



                // outputRow["head_event_id"].Set(head["event_id"].String);
                var custom_tag_list="";
                if (head["custom_tag_list"] != null && !string.IsNullOrEmpty(head["custom_tag_list"].String))
                {
                    custom_tag_list = head["custom_tag_list"].String;
                }

            // HeadEvent Data
            if(!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "pgauth", JsllPageViewHelper.JsonType.JsonObject)))
                outputRow["Author"].Set(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "pgauth", JsllPageViewHelper.JsonType.JsonObject));
            else
                outputRow["Author"].Set(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "ms.author", JsllPageViewHelper.JsonType.JsonObject));
            //no response yet
            outputRow["cpnid"].Set(JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "cpnid", JsllPageViewHelper.JsonType.JsonObject));
            
            if (head["enrich_rip_isp"]!=null &&!string.IsNullOrEmpty(head["enrich_rip_isp"].String))
                outputRow["IsMSInternalTraffic"].Set(string.Equals(head["enrich_rip_isp"].String, "microsoft corporation", StringComparison.InvariantCultureIgnoreCase));

                DateTime lastReviewed;
            var lastReviewedString="";
            if (!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "date", JsllPageViewHelper.JsonType.JsonObject)))
                lastReviewedString = JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "date", JsllPageViewHelper.JsonType.JsonObject);
            else
                lastReviewedString = JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "ms.date", JsllPageViewHelper.JsonType.JsonObject);
            if (!string.IsNullOrEmpty(lastReviewedString) && DateTime.TryParse(lastReviewedString, out lastReviewed))
                    outputRow["LastReviewed"].Set(lastReviewed);

            if (head["enrich_url_pg_query_string"]!=null && !string.IsNullOrEmpty(head["enrich_url_pg_query_string"].String))
                {
                var monikerRegex = Regex.Match(head["enrich_url_pg_query_string"].String, "view=([^#&]*)");
                if (monikerRegex.Success && monikerRegex.Groups.Count >= 2)
                    outputRow["Moniker"].Set(monikerRegex.Groups[1].Value);
            }

            if(!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "product", JsllPageViewHelper.JsonType.JsonObject)))
                outputRow["MSProd"].Set(JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "product", JsllPageViewHelper.JsonType.JsonObject));
            else
                outputRow["MSProd"].Set(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "ms.prod", JsllPageViewHelper.JsonType.JsonObject));

            if(!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "pgsrvcs", JsllPageViewHelper.JsonType.JsonObject)))
                outputRow["MSService"].Set(JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "pgsrvcs", JsllPageViewHelper.JsonType.JsonObject));
            else
                outputRow["MSService"].Set(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "ms.service", JsllPageViewHelper.JsonType.JsonObject));
            if(!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "pgtop", JsllPageViewHelper.JsonType.JsonObject)))
                outputRow["TopicType"].Set(JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "pgtop", JsllPageViewHelper.JsonType.JsonObject));
            else
                outputRow["TopicType"].Set(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "ms.topic", JsllPageViewHelper.JsonType.JsonObject));



            if (head["enrich_url_pg_domain"] != null &&!string.IsNullOrEmpty(head["enrich_url_pg_domain"].String) && head["enrich_url_pg_uri_stem"] != null && !string.IsNullOrEmpty(head["enrich_url_pg_uri_stem"].String))
            {
                outputRow["Url"].Set(string.Format("https://{0}{1}", head["enrich_url_pg_domain"].String, head["enrich_url_pg_uri_stem"].String).ToLower());
            }



            /*Experiment events   Benson:behavior==12?true:false
            
            if (head["Behavior"]!=null &&  head["Behavior"].IntegerQ!=null && head["Behavior"].IntegerQ.Equals(12))
            {
                outputRow["IsExperiment"].Set(true);
                outputRow["ExperimentEventJson"].Set(JsonConvert.SerializeObject(new ExperimentModel()
                {
                    EventDateTime = head["server_utc_datetime"].DateTime,
                    expid = JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "expid", JsllPageViewHelper.JsonType.JsonObject),
                    variationid = JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "variationid", JsllPageViewHelper.JsonType.JsonObject),
                    expname = JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "expname", JsllPageViewHelper.JsonType.JsonObject),
                    variationname = JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "variationname", JsllPageViewHelper.JsonType.JsonObject),
                    expengine = JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "expengine", JsllPageViewHelper.JsonType.JsonObject),
                    expstatus = JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "expstatus", JsllPageViewHelper.JsonType.JsonObject)
                }));
            }
            else
            {
                outputRow["IsExperiment"].Set(false);
            }
            */

            // Experiment events      
            if (!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "experiment_id", JsllPageViewHelper.JsonType.JsonObject)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "experimental", JsllPageViewHelper.JsonType.JsonObject)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "assigned_experiments", JsllPageViewHelper.JsonType.JsonObject)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "toc_experiment_id", JsllPageViewHelper.JsonType.JsonObject)) ||
                !string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "toc_experimental", JsllPageViewHelper.JsonType.JsonObject)))
            {
                outputRow["IsExperiment"].Set(true);
                outputRow["ExperimentEventJson"].Set(JsonConvert.SerializeObject(new ExperimentModel()
                {
                    EventDateTime = head["server_utc_datetime"].DateTime,
                    experimentid = JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "experiment_id", JsllPageViewHelper.JsonType.JsonObject),
                    experimental = JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "experimental", JsllPageViewHelper.JsonType.JsonObject),
                    assigned_experiments = JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "assigned_experiments", JsllPageViewHelper.JsonType.JsonObject),
                    toc_experiment_id = JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "toc_experiment_id", JsllPageViewHelper.JsonType.JsonObject),
                    toc_experimental = JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "toc_experimental", JsllPageViewHelper.JsonType.JsonObject)
                }));
            }
            else
            {
                outputRow["IsExperiment"].Set(false);
            }


            if (head["enrich_url_pg_domain"]!=null && !string.IsNullOrEmpty(head["enrich_url_pg_domain"].String)&& head["enrich_url_pg_domain"].String.Equals("channel9.msdn.com", StringComparison.InvariantCulture))
            {
                if(!string.IsNullOrEmpty(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "pgtype", JsllPageViewHelper.JsonType.JsonObject)))
                    outputRow["Ch9PageType"].Set(JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "pgtype", JsllPageViewHelper.JsonType.JsonObject));
                else
                    outputRow["Ch9PageType"].Set(JsllPageViewHelper.GetValueFromJsonString(head, custom_tag_list, "ms.pagetype", JsllPageViewHelper.JsonType.JsonObject));   
                
                outputRow["Ch9Partnerid"].Set(JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "ms.partnerid", JsllPageViewHelper.JsonType.JsonObject));

                outputRow["Ch9Environment"].Set(JsllPageViewHelper.GetValueFromJsonString(head,custom_tag_list, "ms.env", JsllPageViewHelper.JsonType.JsonObject));
            }

            //F1 Query
            Regex msdnF1 = new Regex(@"^msdn\.microsoft\.com/query/");
            Regex techNetF1 = new Regex(@"^technet\.microsoft\.com/query/");
            Regex docsF1 = new Regex(@"^docs\.microsoft\.com.*\?f1url=");
            string url = string.Format("{0}{1}{2}", head["enrich_url_pg_domain"].String, head["enrich_url_pg_uri_stem"] != null && !string.IsNullOrEmpty(head["enrich_url_pg_uri_stem"].String) ? head["enrich_url_pg_uri_stem"].String:string.Empty, head["enrich_url_pg_query_string"] != null && !string.IsNullOrEmpty(head["enrich_url_pg_query_string"].String) ? head["enrich_url_pg_query_string"].String : string.Empty);

            if (msdnF1.Match(url).Success || techNetF1.Match(url).Success || docsF1.Match(url).Success)
                outputRow["IsF1Query"].Set(true);
            else
                outputRow["IsF1Query"].Set(false);

        }
}
}