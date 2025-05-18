import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Full airline sentiment data
data = {
    "Airline": ["KLM", "AirFrance", "British_Airways", "AmericanAir", "Lufthansa", 
                "AirBerlin", "AirBerlin assist", "easyJet", "RyanAir", 
                "SingaporeAir", "Qantas", "EtihadAirways", "VirginAtlantic"],
    "Positive_original": [5351, 2182, 21476, 33479, 3941, 0, 0, 9276, 6526, 3318, 6365, 2637, 10512],
    "Positive_quoted": [694, 234, 2094, 2966, 345, 0, 0, 855, 774, 352, 989, 285, 1263],
    "Positive_replies": [11297, 3525, 48693, 54904, 7276, 0, 0, 20677, 19666, 6729, 12384, 4266, 28049],
    "Negative_original": [12589, 7056, 74796, 137281, 12228, 0, 0, 48626, 43539, 5446, 13396, 3513, 10077],
    "Negative_quoted": [1888, 509, 4216, 10206, 944, 0, 0, 2711, 3650, 360, 1791, 335, 1042],
    "Negative_replies": [24307, 6968, 82985, 146327, 15113, 0, 0, 52642, 51103, 5679, 25013, 3780, 15360],
    "Neutral_original": [7712, 3555, 34964, 37428, 6014, 0, 0, 22266, 20251, 5171, 7613, 3339, 10513],
    "Neutral_quoted": [833, 275, 2214, 3434, 479, 0, 0, 1227, 1407, 289, 1141, 241, 789],
    "Neutral_replies": [18765, 6486, 74074, 90174, 12401, 0, 0, 46712, 41471, 10000, 21627, 4792, 20331]
}

df = pd.DataFrame(data)
# Sum sentiment counts
df["Total_Positive"] = df["Positive_original"] + df["Positive_quoted"] + df["Positive_replies"]
df["Total_Negative"] = df["Negative_original"] + df["Negative_quoted"] + df["Negative_replies"]
df["Total_Neutral"] = df["Neutral_original"] + df["Neutral_quoted"] + df["Neutral_replies"]

# Plot
df.set_index("Airline")[["Total_Positive", "Total_Negative", "Total_Neutral"]].plot(
    kind="bar", 
    stacked=True, 
    color=["#4CAF50", "#F44336", "#9E9E9E"],
    figsize=(14, 6)
)
plt.title("Total Sentiment Volume per Airline")
plt.ylabel("Tweet Count")
plt.xticks(rotation=45)
plt.legend(title="Sentiment")
plt.tight_layout()
plt.show()