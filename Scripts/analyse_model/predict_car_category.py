import tkinter as tk
from tkinter import messagebox
import joblib
import pandas as pd

# Load the trained model
loaded_model = joblib.load('car_category_prediction_model.pkl')

def predict_category():
    # Retrieve input values from text entry fields
    age = int(age_entry.get())
    sexe = sexe_var.get()
    taux = int(taux_entry.get())
    situationfamiliale = situation_var.get()
    nbenfantsacharge = int(nbenfantsacharge_entry.get())
    deuxiemevoiture = deuxiemevoiture_var.get()
    
    # Create a DataFrame with the correct order of features
    input_data = pd.DataFrame({
        'age': [age],
        'sexe_F': [0],
        'sexe_M': [0],
        'taux': [taux],
        'situationfamiliale_Celibataire': [0],
        'situationfamiliale_En Couple': [0],
        'situationfamiliale_Marie(e)': [0],
        'situationfamiliale_Divorcee': [0],
        'nbenfantsacharge': [nbenfantsacharge],
        'deuxiemevoiture_FALSE': [0],
        'deuxiemevoiture_TRUE': [0]
    })
    
    # Map input values to the corresponding columns
    input_data['age'] = age
    input_data[f'sexe_{sexe}'] = 1
    input_data['taux'] = taux
    input_data[f'situationfamiliale_{situationfamiliale}'] = 1
    input_data['nbenfantsacharge'] = nbenfantsacharge
    input_data[f'deuxiemevoiture_{deuxiemevoiture.upper()}'] = 1
    
    # Reorder columns to match the order used during model training
    input_data = input_data[['age', 'taux', 'nbenfantsacharge', 'sexe_F', 'sexe_M', 'situationfamiliale_Celibataire',
                              'situationfamiliale_Divorcee', 'situationfamiliale_En Couple', 'situationfamiliale_Marie(e)',
                                'deuxiemevoiture_FALSE', 'deuxiemevoiture_TRUE']]
    
    # Make prediction using the loaded model
    prediction = loaded_model.predict(input_data)
    
    # Display the prediction
    messagebox.showinfo("Prediction", f"The predicted category is: {prediction[0]}")

# Create the main window
root = tk.Tk()
root.title("Vehicle Category Predictor")

# Create input fields and labels
age_label = tk.Label(root, text="Age:")
age_label.grid(row=0, column=0, padx=10, pady=5)
age_entry = tk.Entry(root)
age_entry.grid(row=0, column=1, padx=10, pady=5)

sexe_label = tk.Label(root, text="Sexe:")
sexe_label.grid(row=1, column=0, padx=10, pady=5)
sexe_var = tk.StringVar(root)
sexe_var.set("M")  # Default value
sexe_optionmenu = tk.OptionMenu(root, sexe_var, "M", "F")
sexe_optionmenu.grid(row=1, column=1, padx=10, pady=5)

taux_label = tk.Label(root, text="Taux:")
taux_label.grid(row=2, column=0, padx=10, pady=5)
taux_entry = tk.Entry(root)
taux_entry.grid(row=2, column=1, padx=10, pady=5)

situation_label = tk.Label(root, text="Situation Familiale:")
situation_label.grid(row=3, column=0, padx=10, pady=5)
situation_var = tk.StringVar(root)
situation_var.set("Celibataire")  # Default value
situation_optionmenu = tk.OptionMenu(root, situation_var, "Celibataire", "En Couple", "Marie(e)", "Divorcee")
situation_optionmenu.grid(row=3, column=1, padx=10, pady=5)

nbenfantsacharge_label = tk.Label(root, text="Number of Children at Charge:")
nbenfantsacharge_label.grid(row=4, column=0, padx=10, pady=5)
nbenfantsacharge_entry = tk.Entry(root)
nbenfantsacharge_entry.grid(row=4, column=1, padx=10, pady=5)

deuxiemevoiture_label = tk.Label(root, text="Second Car (Yes/No):")
deuxiemevoiture_label.grid(row=5, column=0, padx=10, pady=5)
deuxiemevoiture_var = tk.StringVar(root)
deuxiemevoiture_var.set("FALSE")  # Default value
deuxiemevoiture_optionmenu = tk.OptionMenu(root, deuxiemevoiture_var, "TRUE", "FALSE")
deuxiemevoiture_optionmenu.grid(row=5, column=1, padx=10, pady=5)

# Create predict button
predict_button = tk.Button(root, text="Predict", command=predict_category)
predict_button.grid(row=6, column=0, columnspan=2, padx=10, pady=10)

# Run the application
root.mainloop()
