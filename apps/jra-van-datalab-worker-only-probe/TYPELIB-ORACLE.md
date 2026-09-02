# JV-Link 5.0.0 type-library oracle

Source: authenticated installation's 64-bit `JVDTLab.dll`, queried locally with `LoadTypeLibEx`.
The DLL, registry, credentials, and generated executable are not committed. This redacted surface
contains only public COM names and DISPIDs.

The runtime contains one method that is absent from the JV-Link 4.9.0.1 PDF method table:
`JVSetPayFlag` (DISPID 34). Compatibility coverage therefore uses the runtime type library as the
authoritative 5.0.0 surface.

| DISPID | Member            |
| -----: | ----------------- |
|      1 | JVSetSavePath     |
|      4 | JVInit            |
|      5 | JVClose           |
|      6 | JVSetUIProperties |
|      7 | JVOpen            |
|      8 | JVStatus          |
|      9 | JVRead            |
|     10 | JVRTOpen          |
|     11 | JVCancel          |
|     12 | JVFiledelete      |
|     13 | JVSetServiceKey   |
|     15 | JVSetSaveFlag     |
|     19 | JVSkip            |
|     22 | JVGets            |
|     23 | JVMVPlay          |
|     24 | JVMVCheck         |
|     25 | JVFukuFile        |
|     26 | JVFuku            |
|     27 | JVMVOpen          |
|     28 | JVMVRead          |
|     29 | JVMVPlayWithType  |
|     30 | JVCourseFile      |
|     31 | JVCourseFile2     |
|     33 | JVWatchEvent      |
|     34 | JVSetPayFlag      |
|     35 | JVWatchEventClose |
|     36 | JVMVCheckWithType |

Properties observed: `m_savepath`, `m_servicekey`, `m_saveflag`, `m_JVLinkVersion`,
`m_TotalReadFilesize`, `m_CurrentReadFilesize`, `m_CurrentFileTimeStamp`, `ParentHWnd`, and
`m_payflag`.

Events observed with DISPIDs 1–7: `JVEvtPay`, `JVEvtJockeyChange`, `JVEvtWeather`,
`JVEvtCourseChange`, `JVEvtAvoid`, `JVEvtTimeChange`, and `JVEvtWeight`.
