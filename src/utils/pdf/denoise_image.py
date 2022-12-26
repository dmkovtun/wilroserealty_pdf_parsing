# import cv2
# from os import remove


# def denoise_image(file_path: str):
#     image_bw = cv2.imread(file_path)
#     # convert to grayscale
#     gray = cv2.cvtColor(image_bw, cv2.COLOR_BGR2GRAY)
#     # blur
#     blur = cv2.GaussianBlur(gray, (0, 0), sigmaX=5, sigmaY=5)
#     # divide
#     divide = cv2.divide(gray, blur, scale=255)
#     # otsu threshold
#     thresh = cv2.threshold(divide, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
#     # apply morphology
#     kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
#     morph = cv2.morphologyEx(thresh, cv2.MORPH_CLOSE, kernel)
#     # write result to disk
#     cv2.imwrite(file_path + "_morph.jpg", morph)
#     updated_file = file_path + "_morph.jpg"
#     remove(file_path)
#     return updated_file
